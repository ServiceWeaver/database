package service

import (
	"context"
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

	"bankofanthos_prototype/eval_driver/dbbranch"
	"bankofanthos_prototype/eval_driver/utility"

	"golang.org/x/net/publicsuffix"
)

type Service struct {
	ConfigPaths  []string
	Runs         string
	OutputPath   string
	LogPath      string
	ProdServices []*utility.ProdService
	Branches     map[string]*dbbranch.Branch

	ReqPorts []string
	Request  *Request
}

func Init(curRun int, prodServices []*utility.ProdService, reqPorts []string, branches map[string]*dbbranch.Branch, request *Request, configLoader *utility.ConfigLoader) (*Service, error) {
	service := &Service{
		Runs:         fmt.Sprintf("%d", curRun),
		LogPath:      fmt.Sprintf("%slog%d", configLoader.GetLogPath(), curRun),
		OutputPath:   fmt.Sprintf("%sresp%d", configLoader.GetOutPath(), curRun),
		ProdServices: prodServices,
		Branches:     branches,
		ReqPorts:     reqPorts,
		Request:      request,
	}

	for i := 0; i < len(prodServices); i++ {
		service.ConfigPaths = append(service.ConfigPaths, fmt.Sprintf("%sweaver%d-%d.toml", configLoader.GetConfigPath(), curRun, i))
	}

	// generate config
	for i := 0; i < len(prodServices); i++ {
		err := service.generateConfig(service.ConfigPaths[i], prodServices[i].TestListenPort, prodServices[i])
		if err != nil {
			return service, err
		}
	}

	return service, nil
}

func (s *Service) writeOutput(output, outPath string) error {
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
func (s *Service) generateConfig(configPath, listenPort string, prodService *utility.ProdService) error {
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

	return nil
}

func (s *Service) start(cmdCh chan *exec.Cmd, upCh chan bool, binPath, configPath, logPath string) {
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
}

func (s *Service) stop(cmdCh chan *exec.Cmd, runs int) {
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
				return
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func (s *Service) Run(ctx context.Context) {
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

	err := s.sendRequests(ctx, upCh)
	if err != nil {
		log.Panicf("failed to send req, err=%s", err)
	}
	go s.stop(cmdCh, len(s.ProdServices))
	wg.Wait()
}

func (s *Service) sendHttpReqs(ctx context.Context, client *http.Client, ports []string) error {
	for i, req := range s.Request.httpReq {
		output, err := s.Request.exec(client, &req, ports[i])
		if err != nil {
			return err
		}

		s.writeOutput(output, s.OutputPath)

		// update req id
		for _, branch := range s.Branches {
			if err := branch.IncrementReqId(ctx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Service) sendRequests(ctx context.Context, upCh chan bool) error {
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
				err := s.sendHttpReqs(ctx, &client, s.ReqPorts)
				if err != nil {
					return err
				}
				return nil
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
