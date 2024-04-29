package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"bankofanthos_prototype/eval_driver/dbbranch"
	"bankofanthos_prototype/eval_driver/diff"
	"bankofanthos_prototype/eval_driver/service"
	"bankofanthos_prototype/eval_driver/utility"

	"github.com/jackc/pgx/v4/pgxpool"
)

func runTrail(ctx context.Context, trail *utility.Trail, branchers map[string]*dbbranch.Brancher, v1ProdService, v2ProdService *utility.ProdService, req *service.Request, configLoader *utility.ConfigLoader) (*service.Service, error) {
	branchMap := map[string]*dbbranch.Branch{}
	for name, brancher := range branchers {
		b, err := brancher.Branch(ctx, trail.Name)
		if err != nil {
			return nil, fmt.Errorf("branch %s failed: %v", trail.Name, err)
		}
		defer func() {
			err = b.Commit(ctx)
			if err != nil {
				log.Panicf("Commit %s failed: %v", trail.Name, err)
			}
		}()

		branchMap[name] = b
	}

	// only run binaries is needed for each run
	var prodServices []*utility.ProdService
	if trail.IsControl() {
		prodServices = []*utility.ProdService{v1ProdService}
	} else if trail.IsConaryOnly() {
		prodServices = []*utility.ProdService{v2ProdService}
	} else {
		prodServices = []*utility.ProdService{v1ProdService, v2ProdService}
	}

	s, err := service.Init(trail.Cnt, prodServices, trail.ReqPorts, branchMap, req, configLoader)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Start running %s\n", trail.Name)
	s.Run(ctx)
	return s, nil
}

func printDbDiffs(ctx context.Context, branchers map[string]*dbbranch.Brancher, runName, outPath string, branchA, branchB map[string]*dbbranch.Branch, inlineDiff bool, reqCnt int) {
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
	var configFile string
	var deleteBranches, inlineDiff bool
	flag.StringVar(&configFile, "configFile", "config.toml", "Config file for eval")
	flag.BoolVar(&deleteBranches, "deleteBranches", true, "Delete branches at the end of eval run, only set false for investigation purpose")
	flag.BoolVar(&inlineDiff, "inlineDiff", false, "Whether to use inline diff or not")
	flag.Parse()

	configLoader, err := utility.LoadConfig(configFile)
	if err != nil {
		log.Panicf("load config %s failed: %v", configFile, err)
	}

	prodDbs := configLoader.GetProdDbs()

	// get the service running in prod
	v1ProdService := configLoader.GetStableService()
	v2ProdService := configLoader.GetCanaryService()

	ctx := context.Background()

	request, err := service.NewRequest(configLoader.GetReqPath(), configLoader.GetOrigProdPort())
	if err != nil {
		log.Panicf("Failed to get new request: %v", err)
	}

	trails := utility.GetTrials(request.Count, v1ProdService.TestListenPort, v2ProdService.TestListenPort, configLoader.GetOrigProdPort())

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

	var controlService *service.Service
	for _, trail := range trails {
		service, err := runTrail(ctx, trail, branchers, v1ProdService, v2ProdService, request, configLoader)
		if err != nil {
			log.Panicf("trail run failed: %v", err)
		}
		for _, branch := range service.Branches {
			defer func() {
				if deleteBranches {
					err = branch.Delete(ctx)
					if err != nil {
						log.Panicf("Delete failed: %v", err)
					}
				}
			}()
		}
		if trail.IsControl() {
			controlService = service
		} else {
			_, err = diff.OutputEq(controlService.OutputPath, service.OutputPath)
			if err != nil {
				log.Panicf("Failed to compare two outputs: %v", err)
			}
			printDbDiffs(ctx, branchers, trail.Name, configLoader.GetOutPath(), controlService.Branches, service.Branches, inlineDiff, request.Count)
		}
	}

	fmt.Println("Exiting program...")
}
