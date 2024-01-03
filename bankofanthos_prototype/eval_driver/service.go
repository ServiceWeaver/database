package main

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

type Service struct {
	configPaths  []string
	runs         string
	databasePort string
	listenPorts  []string
	outputPath   string
	dumpDbPath   string
	logPath      string
	prodServices []ProdService

	requestsPorts []string
}

func Init(curRun int, listenPorts []string, dbPort string, prodServices []ProdService, requestsPorts []string) (Service, error) {
	service := Service{
		runs:         fmt.Sprintf("%d", curRun),
		databasePort: dbPort,
		listenPorts:  listenPorts,
		logPath:      fmt.Sprintf("logs/log%d", curRun),
		outputPath:   fmt.Sprintf("out/resp%d", curRun),
		dumpDbPath:   fmt.Sprintf("out/db%d.sql", curRun),
		prodServices: prodServices,

		requestsPorts: requestsPorts,
	}

	for i := 0; i < len(prodServices); i++ {
		service.configPaths = append(service.configPaths, fmt.Sprintf("configs/weaver%d-%d.toml", curRun, i))
	}

	// generate config
	for i := 0; i < len(prodServices); i++ {
		err := service.generateConfig(service.configPaths[i], listenPorts[i], prodServices[i])
		if err != nil {
			return service, err
		}
	}

	return service, nil
}

func (s Service) writeOutput(output, outPath string) error {
	// Open the file for writing
	file, err := os.OpenFile(outPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()

	_, err = file.WriteString(output)
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (s Service) generateConfig(configPath, listenPort string, prodService ProdService) error {
	configByte, err := os.ReadFile(prodService.configPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	configStr := strings.ReplaceAll(string(configByte), prodService.databasePort, s.databasePort)
	configStr = strings.ReplaceAll(configStr, prodService.listenPort, listenPort)

	// Open the file for writing
	file, err := os.Create(configPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()

	_, err = file.WriteString(configStr)
	if err != nil {
		log.Fatal(err)
		return err
	}
	fmt.Printf("Successfully generate config file %s\n", configPath)
	return nil
}

func (s Service) start(cmdCh chan *exec.Cmd, upCh chan bool, binPath, configPath, logPath string) {
	fmt.Printf("Start running service %s, config file %s\n", s.runs, configPath)

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "SERVICEWEAVER_CONFIG="+configPath)

	// open the out file for writing
	outfile, err := os.Create(logPath)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile
	cmd.Stderr = outfile

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
		return
	}

	upCh <- true
	cmdCh <- cmd
	fmt.Println("send cmd to cmdch")

	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
		return
	}

	return
}

func (s Service) stop(cmdCh chan *exec.Cmd, runs int) {
	i := 0
	for {
		select {
		case cmd := <-cmdCh:
			fmt.Println("received cmd")
			err := cmd.Process.Signal(syscall.SIGTERM)
			if err != nil {
				_ = fmt.Errorf("failed to terminate the process, err=%+v", err)
				return
			}
			i++
			if i >= runs {
				fmt.Printf("Stopped the service %s\n", s.runs)
				return
			}
		default:
			time.Sleep(1 * time.Second)
			fmt.Printf("waiting for command\n")
		}
	}
}

func (s Service) run(r listOfReqs) {
	cmdCh := make(chan *exec.Cmd, len(s.prodServices))
	upCh := make(chan bool, len(s.prodServices))
	var wg sync.WaitGroup
	for i, prodService := range s.prodServices {
		wg.Add(1)
		go func(bin string, configPath string, i int) {
			s.start(cmdCh, upCh, bin, configPath, fmt.Sprintf(s.logPath+"-%d", i))
			wg.Done()
		}(prodService.bin, s.configPaths[i], i)
	}

	s.sendRequests(upCh, r)
	go s.stop(cmdCh, len(s.prodServices))
	wg.Wait()

	err := dumpDb(s.databasePort, s.dumpDbPath)
	if err != nil {
		panic(err)
	}
	fmt.Println("Finished running service")
}

func (s Service) sendListOfReqs(client http.Client, rFunc listOfReqs, ports []string) error {
	reqs := rFunc()
	for i, r := range reqs {
		output, err := req(client, ports[i], r)
		if err != nil {
			return err
		}
		s.writeOutput(output, s.outputPath)
	}
	return nil
}

func (s Service) sendRequests(upCh chan bool, r listOfReqs) error {
	options := cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	}
	jar, err := cookiejar.New(&options)
	if err != nil {
		log.Fatal(err)
		return err
	}
	client := http.Client{Jar: jar}

	i := 0
	for {
		select {
		case <-upCh:
			i++
			if i == len(s.configPaths) {
				fmt.Println("Start sending requests")
				err = s.sendListOfReqs(client, r, s.requestsPorts)
				if err != nil {
					return err
				}
				return nil
			}
		default:
			println("Waiting for data")
			time.Sleep(1 * time.Second)
		}
	}
}
