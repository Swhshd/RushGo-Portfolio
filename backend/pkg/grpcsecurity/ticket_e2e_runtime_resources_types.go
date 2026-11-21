package grpcsecurity

// TicketE2ERuntimeResources contains the infrastructure endpoints used by a
// TEST_ONLY service. Tagged binaries validate these against the isolated
// Compose contract before opening a database, cache, or broker connection.
type TicketE2ERuntimeResources struct {
	DatabaseURL  string
	RedisURL     string
	KafkaBrokers []string
}
