package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"bankofanthos_prototype/eval_driver/dbbranch"
	"bankofanthos_prototype/eval_driver/diff"
	"bankofanthos_prototype/eval_driver/service"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	configPath            = "configs/"
	logPath               = "logs/"
	outPath               = "out/"
	nonDeterministicField = "nondeterministic/"
	responseType          = "response"
)

// requestsPorts generates traffic pattern, each request will be directed to either v1 service port
// or v2 service port
func requestsPorts(numOfRuns int, v1Port, v2Port, origListenPort, reqPath string) (*service.Request, [][]string, error) {
	allPorts := [][]string{}
	request, err := service.NewRequest(reqPath, origListenPort)
	if err != nil {
		return nil, nil, err
	}
	for r := 0; r < numOfRuns; r++ {
		ports := []string{}
		if r <= 1 {
			// for all v1 traffic
			for i := 0; i < request.Count; i++ {
				ports = append(ports, v1Port)
			}
			allPorts = append(allPorts, ports)
		}

		if r == 2 {
			// half to v2, half to v1
			for i := 0; i < request.Count/2; i++ {
				ports = append(ports, v2Port)
			}
			for i := request.Count / 2; i < request.Count; i++ {
				ports = append(ports, v1Port)
			}
			allPorts = append(allPorts, ports)
		}
	}

	return request, allPorts, nil
}

func getDatabaseFromURL(databaseUrl string) (*service.Database, error) {
	posS := strings.LastIndex(databaseUrl, "/")
	posE := strings.Index(databaseUrl, "?")

	if posS == -1 {
		return nil, fmt.Errorf("database name not found in URL")
	}

	return &service.Database{Name: databaseUrl[posS+1 : posE], Url: databaseUrl}, nil
}

func runTrail(ctx context.Context, namespace string, branchers map[string]*dbbranch.Brancher, runCnt int, listenPorts []string, prodServices []service.ProdService, reqPorts []string, req *service.Request) (*service.Service, error) {
	branchMap := map[string]*dbbranch.Branch{}
	for name, brancher := range branchers {
		b, err := brancher.Branch(ctx, namespace)
		if err != nil {
			return nil, fmt.Errorf("branch %s failed: %v", namespace, err)
		}
		defer func() {
			err = b.Commit(ctx)
			if err != nil {
				log.Panicf("Commit %s failed: %v", namespace, err)
			}
		}()

		branchMap[name] = b
	}

	trail, err := service.Init(runCnt, listenPorts, prodServices, reqPorts, branchMap, req)
	if err != nil {
		log.Panicf("Init service failed: %v", err)
	}
	trail.Run(ctx)
	return trail, nil
}

func printDbDiffs(ctx context.Context, branchers map[string]*dbbranch.Brancher, runName string, branchA, branchB map[string]*dbbranch.Branch, inlineDiff bool, reqCnt int) {
	f, err := os.Create(fmt.Sprintf("%sDiffPerReq_%s", outPath, runName))
	if err != nil {
		log.Panicf("Failed to create file: %v", err)
	}
	defer f.Close()

	for name, brancher := range branchers {
		branchDiffs, err := brancher.ComputeDiffAtN(ctx, branchA[name], branchB[name], reqCnt)
		if err != nil {
			log.Panicf("failed to compute diff: %v", err)
		}
		dbDiffOut, err := diff.DisplayDiff(branchDiffs, inlineDiff)
		if err != nil {
			log.Panicf("failed to display inline diff: %v", err)
		}
		fmt.Println(dbDiffOut)

		branchDiffPerReqs, err := brancher.ComputeDiffPerReq(ctx, branchA[name], branchB[name], reqCnt)
		if err != nil {
			log.Panicf("failed to compute diff: %v", err)
		}
		for n, diffPerReq := range branchDiffPerReqs {
			dbDiffOutPerReq, err := diff.DisplayDiff(diffPerReq, inlineDiff)
			if err != nil {
				log.Panicf("failed to display diff per req: %v", err)
			}
			fmt.Fprintf(f, "[%d]\n%s\n", n, dbDiffOutPerReq)
		}
	}
}

func main() {
	// parse flags
	var origListenPort, v2Port, v1Port, dbUrls, reqPath, v1Bin, v2Bin, v1Config, v2Config string
	var deleteBranches, inlineDiff bool

	flag.StringVar(&origListenPort, "origListenPort", "9000", "Listen port for original service.")
	flag.StringVar(&v1Port, "v1Port", "9001", "Listen port for stable v1 service.")
	flag.StringVar(&v2Port, "v2Port", "9002", "Listen port for canary v2 service.")
	flag.StringVar(&v1Bin, "v1Bin", "./../bankofanthos/bankofanthos_demo", "Stable v1 binary")
	flag.StringVar(&v2Bin, "v2Bin", "./../bankofanthos/bankofanthos_demo", "Canary v2 binary")
	flag.StringVar(&v1Config, "v1Config", "../bankofanthos/weaver.toml", "Stable v1 config")
	flag.StringVar(&v2Config, "v2Config", "../bankofanthos/weaver_canary.toml", "Canary v2 config")
	flag.StringVar(&reqPath, "reqPath", "../tester/reqlog_demo.json", "Requests for eval to run.")
	flag.StringVar(&dbUrls, "dbUrls", "postgresql://admin:admin@localhost:5432/accountsdb?sslmode=disable,postgresql://admin:admin@localhost:5432/postgresdb?sslmode=disable", "database urls used for app; split by ,")
	flag.BoolVar(&deleteBranches, "deleteBranches", true, "Delete branches at the end of eval run, only set false for investigation purpose")
	flag.BoolVar(&inlineDiff, "inlineDiff", false, "Whether to use inline diff or not")
	flag.Parse()

	// create directories to store eval info
	dirs := []string{configPath, logPath, outPath, nonDeterministicField}
	for _, dir := range dirs {
		err := os.RemoveAll(dir)
		if err != nil {
			log.Panicf("Remove %s failed: %v", dir, err)
		}
		err = os.Mkdir(dir, 0700)
		if err != nil {
			log.Panicf("Mkdir %s failed: %v", dir, err)
		}
	}

	// get prod snapshot database
	urlSlice := strings.Split(dbUrls, ",")
	prodDbs := map[string]*service.Database{}
	for _, url := range urlSlice {
		db, err := getDatabaseFromURL(url)
		if err != nil {
			log.Panicf("Parse databse url %s failed: %v", url, err)
		}
		prodDbs[db.Name] = db
	}

	// get the service running in prod
	stableProdService := service.ProdService{
		ConfigPath: v1Config,
		ListenPort: origListenPort,
		Bin:        v1Bin,
	}
	canaryProdService := service.ProdService{
		ConfigPath: v2Config,
		ListenPort: origListenPort,
		Bin:        v2Bin,
	}

	ctx := context.Background()
	runCnt := 0
	totalRun := 3
	// generate traffic patterns for request
	request, allPorts, err := requestsPorts(totalRun, v1Port, v2Port, origListenPort, reqPath)
	if err != nil {
		log.Panicf("Failed to get new request: %v", err)
	}

	branchers := map[string]*dbbranch.Brancher{}
	for _, prodDb := range prodDbs {
		db, err := pgxpool.Connect(ctx, prodDb.Url)
		if err != nil {
			log.Panicf("Connect to DB %s failed with %s: %v", prodDb.Name, prodDb.Url, err)
		}
		defer db.Close()
		brancher := dbbranch.NewBrancher(db)
		branchers[prodDb.Name] = brancher
	}

	controlService, err := runTrail(ctx, "Control", branchers, runCnt, []string{v1Port}, []service.ProdService{stableProdService}, allPorts[runCnt], request)
	if err != nil {
		log.Panicf("trail run failed: %v", err)
	}
	for _, branch := range controlService.Branches {
		defer func() {
			if deleteBranches {
				err = branch.Delete(ctx)
				if err != nil {
					log.Panicf("Delete failed: %v", err)
				}
			}
		}()
	}

	runCnt += 1

	controlService2, err := runTrail(ctx, "ControlTwo", branchers, runCnt, []string{v1Port}, []service.ProdService{stableProdService}, allPorts[runCnt], request)
	if err != nil {
		log.Panicf("trail run failed: %v", err)
	}
	for _, branch := range controlService2.Branches {
		defer func() {
			if deleteBranches {
				err = branch.Delete(ctx)
				if err != nil {
					log.Panicf("Delete failed: %v", err)
				}
			}
		}()
	}

	if err := diff.GetNonDeterministic(controlService, controlService2); err != nil {
		log.Panicf("Get non deterministic error failed: %v", err)
	}

	// // run experimental service, all traffic send to canary binary
	// runCnt += 1
	// experimentalCanaryNamespace := "E_C"
	// experimentalCanaryService, err := runTrail(ctx, experimentalCanaryNamespace, branchers, runCnt, []string{v2Port}, []service.ProdService{canaryProdService}, allPorts[runCnt], request)
	// if err != nil {
	// 	log.Panicf("trail run failed: %v", err)
	// }
	// for _, branch := range experimentalCanaryService.Branches {
	// 	defer func() {
	// 		if deleteBranches {
	// 			err = branch.Delete(ctx)
	// 			if err != nil {
	// 				log.Panicf("Delete failed: %v", err)
	// 			}
	// 		}
	// 	}()
	// }

	// _, err = diff.OutputEq(controlService.OutputPath, experimentalCanaryService.OutputPath, responseType)
	// if err != nil {
	// 	log.Panicf("Failed to compare two outputs: %v", err)
	// }

	// printDbDiffs(ctx, branchers, experimentalCanaryNamespace, controlService.Branches, experimentalCanaryService.Branches, inlineDiff, request.Count)

	// // run requests half on stable (v1) half on canary (v2)
	// runCnt += 1
	// experimentalSCNamespace := "E_SC"
	// experimentalSCService, err := runTrail(ctx, experimentalSCNamespace, branchers, runCnt, []string{v1Port, v2Port}, []service.ProdService{stableProdService, canaryProdService}, allPorts[runCnt], request)
	// if err != nil {
	// 	log.Panicf("trail run failed: %v", err)
	// }
	// for _, branch := range experimentalSCService.Branches {
	// 	defer func() {
	// 		if deleteBranches {
	// 			err = branch.Delete(ctx)
	// 			if err != nil {
	// 				log.Panicf("Delete failed: %v", err)
	// 			}
	// 		}
	// 	}()
	// }

	// _, err = diff.OutputEq(controlService.OutputPath, experimentalSCService.OutputPath, responseType)
	// if err != nil {
	// 	log.Panicf("Failed to compare two outputs: %v", err)
	// }

	// printDbDiffs(ctx, branchers, experimentalSCNamespace, controlService.Branches, experimentalSCService.Branches, inlineDiff, request.Count)

	// run requests half on canary (v2) half on stable (v1)
	runCnt += 1
	experimentalCSNamespace := "E_CS"
	experimentalCSService, err := runTrail(ctx, experimentalCSNamespace, branchers, runCnt, []string{v1Port, v2Port}, []service.ProdService{stableProdService, canaryProdService}, allPorts[runCnt], request)
	if err != nil {
		log.Panicf("trail run failed: %v", err)
	}
	for _, branch := range experimentalCSService.Branches {
		defer func() {
			if deleteBranches {
				err = branch.Delete(ctx)
				if err != nil {
					log.Panicf("Delete failed: %v", err)
				}
			}
		}()
	}

	_, err = diff.OutputEq(controlService.OutputPath, experimentalCSService.OutputPath, responseType)
	if err != nil {
		log.Panicf("Failed to compare two outputs: %v", err)
	}

	printDbDiffs(ctx, branchers, experimentalCSNamespace, controlService.Branches, experimentalCSService.Branches, inlineDiff, request.Count)

	fmt.Println("Exiting program...")
}
