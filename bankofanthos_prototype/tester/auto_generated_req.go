package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type metadata struct {
	ReqCount int `json:"ReqCount"`
}

type jsonFormat struct {
	HttpReqs []httpReq `json:"HttpReqs"`
	Metadata metadata  `json:"Metadata"`
}

type httpReq struct {
	Body   url.Values `json:"Body"`
	Method string     `json:"Method"`
	Url    string     `json:"Url"`
}

type generator struct {
	f        *os.File
	httpReqs []httpReq
	counts   []int
}
type user struct {
	AccountId string
	Username  string
	Password  string
	Firstname string
	Lastname  string
	Balance   int
}

type signUp struct {
	Username  string
	Password  string
	Firstname string
	Lastname  string
	Address   string
	Country   string
	State     string
	Zip       string
	Ssn       string
	Birthday  string
	Timezone  int
}

type send struct {
	acctNum    string
	routingNum string
	amount     int
	label      string
}

type deposit struct {
	acctNum    string
	routingNum string
	amount     int
	label      string
}

func newGenerator(counts []int) (*generator, error) {
	f, err := os.Create(reqLog)
	if err != nil {
		return nil, err
	}

	return &generator{f: f, counts: counts}, nil
}

func randStrL(minLen, maxLen int) string {
	rand.NewSource(time.Now().UnixNano())
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := rand.Intn(maxLen-minLen+1) + minLen
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

// random generate a int length between min and max
func randIntL(minLen, maxLen int) int {
	rand.NewSource(time.Now().UnixNano())

	min := int(math.Pow10(minLen - 1))
	max := int(math.Pow10(maxLen) - 1)

	return min + rand.Intn(max-min+1)
}

// random generate a int between min and max
func randIntn(min, max int) int {
	rand.NewSource(time.Now().UnixNano())
	return rand.Intn(max-min+1) + min
}

func (g *generator) signUp() (*user, error) {
	signUp := &signUp{
		Username:  randStrL(3, 8),
		Password:  randStrL(3, 8),
		Firstname: randStrL(3, 8),
		Lastname:  randStrL(3, 8),
		Address:   fmt.Sprintf("%d %dth Avenue, New York City", randIntn(1, 200), randIntn(1, 20)),
		Country:   `United States`,
		State:     `NY`,
		Zip:       fmt.Sprintf("%d", randIntL(5, 5)),
		Ssn:       fmt.Sprintf("%d-%d-%d", randIntL(3, 3), randIntL(2, 2), randIntL(4, 4)),
		Birthday:  fmt.Sprintf("%d-%d-%d", randIntn(1900, 2024), randIntn(1, 12), randIntn(1, 20)),
		Timezone:  -5,
	}

	params := url.Values{}
	params.Add("username", signUp.Username)
	params.Add("password", signUp.Password)
	params.Add("password-repeat", signUp.Password)
	params.Add("firstname", signUp.Firstname)
	params.Add("lastname", signUp.Lastname)
	params.Add("address", signUp.Address)
	params.Add("country", signUp.Country)
	params.Add("state", signUp.State)
	params.Add("zip", signUp.Zip)
	params.Add("ssn", signUp.Ssn)
	params.Add("birthday", signUp.Birthday)
	params.Add("timezone", fmt.Sprintf("%d", signUp.Timezone))

	user := &user{
		Username:  signUp.Username,
		Password:  signUp.Password,
		Firstname: signUp.Firstname,
		Lastname:  signUp.Lastname,
	}
	req := httpReq{Body: params, Url: "http://localhost:9000/signup", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return user, nil
}

func (g *generator) login(user *user) error {
	params := url.Values{}
	params.Add("username", user.Username)
	params.Add("password", user.Password)

	req := httpReq{Body: params, Url: "http://localhost:9000/login", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) logout() error {
	params := url.Values{}

	req := httpReq{Body: params, Url: "http://localhost:9000/logout", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) deposit(user *user) error {
	deposit := &deposit{
		acctNum:    fmt.Sprintf("%d", randIntL(10, 10)),
		routingNum: fmt.Sprintf("%d", randIntL(9, 9)),
		amount:     randIntL(1, 6),
		label:      randStrL(0, 4),
	}
	user.Balance += deposit.amount

	params := url.Values{}
	params.Add("account", `add`)
	params.Add("external_account_num", deposit.acctNum)
	params.Add("external_routing_num", deposit.routingNum)
	params.Add("external_label", deposit.label)
	params.Add("amount", fmt.Sprintf("%d", deposit.amount))
	params.Add("uuid", uuid.New().String())

	req := httpReq{Body: params, Url: "http://localhost:9000/deposit", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) send(user *user) error {
	// randSend generates send amount between [0, user.balance]
	randSend := func() int {
		rand.NewSource(time.Now().UnixNano())
		return rand.Intn(user.Balance + 1)
	}
	send := &send{
		acctNum:    fmt.Sprintf("%d", randIntL(10, 10)),
		routingNum: fmt.Sprintf("%d", randIntL(9, 9)),
		amount:     randSend(),
		label:      randStrL(0, 4),
	}
	user.Balance -= send.amount

	params := url.Values{}
	params.Add("account_num", `add`)
	params.Add("contact_account_num", send.acctNum)
	params.Add("external_routing_num", send.routingNum)
	params.Add("contact_label", send.label)
	params.Add("amount", strconv.Itoa(send.amount))
	params.Add("uuid", uuid.New().String())

	req := httpReq{Body: params, Url: "http://localhost:9000/payment", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) write() error {
	metadata := metadata{ReqCount: len(g.httpReqs)}
	jsonFormat := &jsonFormat{
		Metadata: metadata,
		HttpReqs: g.httpReqs,
	}
	b, err := json.MarshalIndent(jsonFormat, "", "  ")
	if err != nil {
		return err
	}

	g.f.Write(b)
	return nil
}

// generateReqPerUser generates n count request for one new user
// request method will always be: signup, login, deposit... , send..., logout
func (g *generator) generateReqPerUser(n int) error {
	n -= 3 // decrease signup/login/logout count

	user, err := g.signUp()
	if err != nil {
		return err
	}
	if err := g.login(user); err != nil {
		return err
	}

	for i := 0; i <= n/2; i++ {
		if err := g.deposit(user); err != nil {
			return err
		}
	}

	for i := n/2 + 1; i < n; i++ {
		if err := g.send(user); err != nil {
			return err
		}
	}

	return g.logout()
}

func (g *generator) generate() error {
	for _, c := range g.counts {
		if err := g.generateReqPerUser(c); err != nil {
			log.Panicf("failed to generate requests per user, err=%s", err)
		}
	}
	return g.write()
}
