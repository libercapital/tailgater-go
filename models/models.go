package tg_models

type AmqpConfig struct {
	Host     string
	User     string
	Password string
	Protocol string
	VHost    string
}

type DatabaseConfig struct {
	DbHost     string
	DbDatabase string
	DbUser     string
	DbPassword string
	DbPort     string
}
