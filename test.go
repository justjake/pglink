package main

import "fmt"
import "reflect"
import "runtime"

var specs = []string{
	"%v",
	"%#v",
	"%+v",
	"%T",
}

type byteType byte

func (b byteType) String() string {
	return string(b) + " :)"
}

type some struct{}
type SliceType []byteType

func (s some) method(a string) {
	fmt.Println("hi", a)
}

func debug(name string, v any) {
	fmt.Printf("-- %s --\n", name)
	for _, s := range specs {
		fmt.Printf("%s: ", s)
		fmt.Printf(s+"\n", v)
	}
	r := reflect.ValueOf(v)
	if r.Kind() == reflect.Func {
		name := runtime.FuncForPC(r.Pointer()).Name()
		fmt.Printf("runtime.FuncForPC.Name: %v\n", name)
	}
}

func main() {
	likeBytes := []byteType{'a', 'b'}
	fmt.Println(likeBytes)
	likeBytesTyped := SliceType(likeBytes)
	debug("SliceType", likeBytesTyped)

	// bytes := likeBytes.([]byte)
	// fmt.Println(bytes)

	// debug("func literal", f)
	// s := some{}
	// debug("some method", s.method)
	// debug("some", s)
}
