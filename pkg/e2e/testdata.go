package e2e

// TestUser represents credentials for connecting through pglink.
type TestUser struct {
	Username string
	Password string
}

// PredefinedUsers are the users configured in the test setup (pglink.json).
var PredefinedUsers = struct {
	App       TestUser
	Admin     TestUser
	Developer TestUser
}{
	App:       TestUser{Username: "app", Password: "app_password"},
	Admin:     TestUser{Username: "admin", Password: "admin_password"},
	Developer: TestUser{Username: "developer", Password: "developer_password"},
}

// Backend represents a PostgreSQL backend server running in docker-compose.
type Backend struct {
	Name string // Container name (e.g., "alpha")
	Port int    // Host port (e.g., 15432)
}

// PredefinedBackends are the PostgreSQL backends defined in docker-compose.yaml.
var PredefinedBackends = []Backend{
	{Name: "alpha", Port: 15432},
	{Name: "bravo", Port: 15433},
	{Name: "charlie", Port: 15434},
}

// TestDatabase represents a database accessible through pglink.
type TestDatabase struct {
	Name        string // Name as exposed through pglink (e.g., "alpha_uno")
	BackendHost string // Backend postgres host
	BackendPort int    // Backend postgres port
	BackendDB   string // Actual database name on backend (e.g., "uno")
}

// PredefinedDatabases are the databases configured in pglink.json.
var PredefinedDatabases = []TestDatabase{
	{Name: "alpha_uno", BackendHost: "localhost", BackendPort: 15432, BackendDB: "uno"},
	{Name: "alpha_dos", BackendHost: "localhost", BackendPort: 15432, BackendDB: "dos"},
	{Name: "bravo_uno", BackendHost: "localhost", BackendPort: 15433, BackendDB: "uno"},
	{Name: "bravo_dos", BackendHost: "localhost", BackendPort: 15433, BackendDB: "dos"},
	{Name: "charlie_uno", BackendHost: "localhost", BackendPort: 15434, BackendDB: "uno"},
	{Name: "charlie_dos", BackendHost: "localhost", BackendPort: 15434, BackendDB: "dos"},
}

// GetTestDatabase returns the TestDatabase config for the given database name.
// Returns nil if not found.
func GetTestDatabase(name string) *TestDatabase {
	for _, db := range PredefinedDatabases {
		if db.Name == name {
			return &db
		}
	}
	return nil
}
