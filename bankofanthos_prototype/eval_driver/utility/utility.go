package utility

type Database struct {
	Name string
	Url  string
}

// ProdService defines binary will be running in prod
type ProdService struct {
	ConfigPath     string
	Bin            string
	ListenPort     string // prod listen port
	TestListenPort string // listen port used for eval test
}
