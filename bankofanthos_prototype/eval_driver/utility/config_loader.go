package utility

import (
	"os"

	"github.com/pelletier/go-toml"
)

type generatedPath struct {
	DirPath    string
	ConfigPath string
	LogPath    string
	OutPath    string
}

type info struct {
	ProdPort                 string
	Databases                map[string]string
	RequestPath              string
	NonDeterministicDbFields map[string]map[string][]string
}

type testService struct {
	Port   string
	Bin    string
	Config string
}

type ConfigLoader struct {
	GeneratedPath generatedPath
	Info          info
	Stable        testService
	Canary        testService
}

func (c *ConfigLoader) createGeneatedDir() error {
	err := os.RemoveAll(c.GeneratedPath.DirPath)
	if err != nil {
		return err
	}

	dirs := []string{c.GeneratedPath.DirPath, c.GeneratedPath.ConfigPath, c.GeneratedPath.LogPath, c.GeneratedPath.OutPath}
	for _, dir := range dirs {
		err = os.Mkdir(dir, 0700)
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadConfig(config string) (*ConfigLoader, error) {
	var configs ConfigLoader
	f, err := os.ReadFile(config)
	if err != nil {
		return nil, err
	}

	if err = toml.Unmarshal(f, &configs); err != nil {
		return nil, err
	}

	err = configs.createGeneatedDir()
	return &configs, err
}

func (c *ConfigLoader) GetProdDbs() map[string]*Database {
	prodDbs := map[string]*Database{}
	for name, url := range c.Info.Databases {
		prodDbs[name] = &Database{Name: name, Url: url}
	}

	return prodDbs
}

func (c *ConfigLoader) GetCanaryService() *ProdService {
	return &ProdService{
		ConfigPath:     c.Canary.Config,
		ListenPort:     c.Info.ProdPort,
		Bin:            c.Canary.Bin,
		TestListenPort: c.Canary.Port,
	}
}

func (c *ConfigLoader) GetStableService() *ProdService {
	return &ProdService{
		ConfigPath:     c.Stable.Config,
		ListenPort:     c.Info.ProdPort,
		Bin:            c.Stable.Bin,
		TestListenPort: c.Stable.Port,
	}
}

func (c *ConfigLoader) GetOrigProdPort() string {
	return c.Info.ProdPort
}

func (c *ConfigLoader) GetReqPath() string {
	return c.Info.RequestPath
}

func (c *ConfigLoader) GetOutPath() string {
	return c.GeneratedPath.OutPath
}

func (c *ConfigLoader) GetLogPath() string {
	return c.GeneratedPath.LogPath
}

func (c *ConfigLoader) GetConfigPath() string {
	return c.GeneratedPath.ConfigPath
}

func (c *ConfigLoader) GetNonDeterministicField() map[string]map[string][]string {
	return c.Info.NonDeterministicDbFields
}
