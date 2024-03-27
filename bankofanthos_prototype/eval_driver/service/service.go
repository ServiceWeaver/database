package service

import (
	"fmt"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/publicsuffix"
)

var (
	configPath = "configs/"
	logPath    = "logs/"
	outPath    = "out/"
)

// ProdService defines binary will be running in prod
type ProdService struct {
	ConfigPath string
	Bin        string
	ListenPort string
}

type Service struct {
	ConfigPaths  []string
	Runs         string
	ListenPorts  []string
	OutputPath   string
	DumpDbPath   string
	LogPath      string
	ProdServices []ProdService

	ReqPorts []string
}

func Init(curRun int, listenPorts []string, prodServices []ProdService, reqPorts []string) (*Service, error) {
	service := &Service{
		Runs:         fmt.Sprintf("%d", curRun),
		ListenPorts:  listenPorts,
		LogPath:      fmt.Sprintf("%slog%d", logPath, curRun),
		OutputPath:   fmt.Sprintf("%sresp%d", outPath, curRun),
		DumpDbPath:   fmt.Sprintf("%sdb%d.sql", outPath, curRun),
		ProdServices: prodServices,

		ReqPorts: reqPorts,
	}

	for i := 0; i < len(prodServices); i++ {
		service.ConfigPaths = append(service.ConfigPaths, fmt.Sprintf("%sweaver%d-%d.toml", configPath, curRun, i))
	}

	// generate config
	for i := 0; i < len(prodServices); i++ {
		err := service.generateConfig(service.ConfigPaths[i], listenPorts[i], prodServices[i])
		if err != nil {
			return service, err
		}
	}

	return service, nil
}

func (s Service) writeOutput(output, outPath string) error {
	file, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(output)
	if err != nil {
		return err
	}

	return nil
}

// generateConfig creates a config file for each run with snapshot database url
func (s Service) generateConfig(configPath, listenPort string, prodService ProdService) error {
	configByte, err := os.ReadFile(prodService.ConfigPath)
	if err != nil {
		return err
	}

	configStr := strings.ReplaceAll(string(configByte), prodService.ListenPort, listenPort)

	file, err := os.Create(configPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(configStr)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully generate config file %s\n", configPath)
	return nil
}

func (s Service) start(cmdCh chan *exec.Cmd, upCh chan bool, binPath, configPath, logPath string) {
	fmt.Printf("Start running service %s, config file %s\n", s.Runs, configPath)

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "SERVICEWEAVER_CONFIG="+configPath)

	// open the out file for writing
	outfile, err := os.Create(logPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile
	cmd.Stderr = outfile

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	upCh <- true
	cmdCh <- cmd

	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}

	return
}

func (s Service) stop(cmdCh chan *exec.Cmd, runs int) {
	i := 0
	for {
		select {
		case cmd := <-cmdCh:
			err := cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				fmt.Printf("Failed to terminate the process: %v\n", err)
				return
			}
			i++
			if i >= runs {
				fmt.Printf("Stopped service %s\n", s.Runs)
				return
			}
		default:
			time.Sleep(1 * time.Second)
			fmt.Println("Waiting for command")
		}
	}
}

func (s Service) Run(r ListOfReqs) {
	cmdCh := make(chan *exec.Cmd, len(s.ProdServices))
	upCh := make(chan bool, len(s.ProdServices))
	var wg sync.WaitGroup
	for i, prodService := range s.ProdServices {
		wg.Add(1)
		go func(bin string, configPath string, i int) {
			s.start(cmdCh, upCh, bin, configPath, fmt.Sprintf(s.LogPath+"-%d", i))
			wg.Done()
		}(prodService.Bin, s.ConfigPaths[i], i)
	}

	s.sendRequests(upCh, r)
	go s.stop(cmdCh, len(s.ProdServices))
	wg.Wait()

	err := dumpDb(s.DumpDbPath)
	if err != nil {
		log.Fatalf("Failed to dump db: %v", err)
	}

	fmt.Println("Finished running service")
}

func (s Service) sendListOfReqs(client http.Client, rFunc ListOfReqs, ports []string) error {
	reqs := rFunc()
	for i, r := range reqs {
		output, err := req(client, ports[i], r)
		if err != nil {
			return err
		}
		s.writeOutput(output, s.OutputPath)
	}

	return nil
}

func (s Service) sendRequests(upCh chan bool, r ListOfReqs) error {
	options := cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	}
	jar, err := cookiejar.New(&options)
	if err != nil {
		return err
	}
	client := http.Client{Jar: jar}

	i := 0
	for {
		select {
		case <-upCh:
			i++
			if i == len(s.ConfigPaths) {
				fmt.Println("Start sending requests")
				err = s.sendListOfReqs(client, r, s.ReqPorts)
				if err != nil {
					return err
				}
				return nil
			}
		default:
			println("Waiting for service up")
			time.Sleep(1 * time.Second)
		}
	}
}
