package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"

	"bankofanthos_prototype/eval_driver/service"

	"github.com/google/uuid"
)

type generator struct {
	f        *os.File
	httpReqs []service.HttpReq
	counts   []int
	users    []*user
}
type user struct {
	AccountId string
	Username  string
	Password  string
	Firstname string
	Lastname  string
	Balance   int
}

func newGenerator(counts []int) (*generator, error) {
	f, err := os.Create(reqLog)
	if err != nil {
		return nil, err
	}

	return &generator{f: f, counts: counts}, nil
}

func randStrL(minLen, maxLen int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	length := randIntn(minLen, maxLen)
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

// randIntL returns a random n-digit integer where n is in the range [min, max]. For example, randIntL(2, 5) returns numbers in the range [10, 99999].
func randIntL(minLen, maxLen int) int {
	min := int(math.Pow10(minLen - 1))
	max := int(math.Pow10(maxLen) - 1)

	return randIntn(min, max)
}

// randIntn returns an integer chosen equiprobably from the range [min, max].
func randIntn(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func (g *generator) signUp() (*user, error) {
	username := randStrL(3, 8)
	password := randStrL(3, 8)
	firstname := randStrL(3, 8)
	lastname := randStrL(3, 8)

	params := url.Values{}
	params.Add("username", username)
	params.Add("password", password)
	params.Add("password-repeat", password)
	params.Add("firstname", firstname)
	params.Add("lastname", lastname)
	params.Add("address", fmt.Sprintf("%d %dth Avenue, New York City", randIntn(1, 200), randIntn(1, 20)))
	params.Add("country", `United States`)
	params.Add("state", `NY`)
	params.Add("zip", fmt.Sprintf("%d", randIntL(5, 5)))
	params.Add("ssn", fmt.Sprintf("%d-%d-%d", randIntL(3, 3), randIntL(2, 2), randIntL(4, 4)))
	params.Add("birthday", fmt.Sprintf("%d-%d-%d", randIntn(1900, 2024), randIntn(1, 12), randIntn(1, 20)))
	params.Add("timezone", fmt.Sprintf("%d", randIntn(-12, 14)))

	req := service.HttpReq{Body: params, Url: "http://localhost:9000/signup", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)

	return &user{
		Username:  username,
		Password:  password,
		Firstname: firstname,
		Lastname:  lastname,
	}, nil
}

func (g *generator) login(user *user) error {
	params := url.Values{}
	params.Add("username", user.Username)
	params.Add("password", user.Password)

	req := service.HttpReq{Body: params, Url: "http://localhost:9000/login", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) logout() error {
	params := url.Values{}

	req := service.HttpReq{Body: params, Url: "http://localhost:9000/logout", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) deposit(user *user) error {
	amount := randIntL(1, 6)
	user.Balance += amount

	params := url.Values{}
	params.Add("account", `add`)
	params.Add("external_account_num", fmt.Sprintf("%d", randIntL(10, 10)))
	params.Add("external_routing_num", fmt.Sprintf("%d", randIntL(9, 9)))
	params.Add("external_label", randStrL(0, 4))
	params.Add("amount", fmt.Sprintf("%d", amount))
	params.Add("uuid", uuid.New().String())

	req := service.HttpReq{Body: params, Url: "http://localhost:9000/deposit", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) send(user *user) error {
	amount := rand.Intn(user.Balance + 1)

	user.Balance -= amount

	params := url.Values{}
	params.Add("account_num", `add`)
	params.Add("contact_account_num", fmt.Sprintf("%d", randIntL(10, 10)))
	params.Add("external_routing_num", fmt.Sprintf("%d", randIntL(9, 9)))
	params.Add("contact_label", randStrL(0, 4))
	params.Add("amount", strconv.Itoa(amount))
	params.Add("uuid", uuid.New().String())

	req := service.HttpReq{Body: params, Url: "http://localhost:9000/payment", Method: "POST"}
	g.httpReqs = append(g.httpReqs, req)
	return nil
}

func (g *generator) write() error {
	jsonFormat := &service.ReqJson{
		HttpReqs: g.httpReqs,
	}
	b, err := json.MarshalIndent(jsonFormat, "", "  ")
	if err != nil {
		return err
	}

	g.f.Write(b)
	return nil
}

// generateReqPerUser generates n deposit/withdraws request for one new user
// request method will always be: login, deposit... , send..., logout
func (g *generator) generateReqPerUser(n int, user *user) error {
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

func (g *generator) generateUser() (*user, error) {
	user, err := g.signUp()
	if err != nil {
		return nil, err
	}

	return user, g.logout()
}

func (g *generator) generate() error {
	for range len(g.counts) {
		user, err := g.generateUser()
		if err != nil {
			return err
		}
		g.users = append(g.users, user)
	}

	for i, c := range g.counts {
		if err := g.generateReqPerUser(c, g.users[i]); err != nil {
			return fmt.Errorf("failed to generate requests per user, err=%s", err)
		}
	}
	return g.write()
}
