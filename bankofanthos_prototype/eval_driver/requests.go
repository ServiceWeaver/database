package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type signUpStruct struct {
	username  string
	password  string
	firstname string
	lastname  string
}

func signupReq(client http.Client, port string, signUp signUpStruct) (string, error) {
	params := url.Values{}
	params.Add("username", signUp.username)
	params.Add("password", signUp.password)
	params.Add("password-repeat", signUp.password)
	params.Add("firstname", signUp.firstname)
	params.Add("lastname", signUp.lastname)
	params.Add("address", `123+Nth+Avenue%2C+New+York+City`)
	params.Add("country", `United+States`)
	params.Add("state", `NY`)
	params.Add("zip", `10004`)
	params.Add("ssn", `111-22-3333`)
	params.Add("birthday", `2023-12-14`)
	params.Add("timezone", `-5`)
	body := strings.NewReader(params.Encode())

	req, err := http.NewRequest("POST", "http://localhost:"+port+"/signup", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(respBody)

	return bodyString, nil
}

type loginStruct struct {
	username string
	password string
}

func loginReq(client http.Client, port string, login loginStruct) (string, error) {
	params := url.Values{}
	params.Add("username", login.username)
	params.Add("password", login.password)
	body := strings.NewReader(params.Encode())

	req, err := http.NewRequest("POST", "http://localhost:"+port+"/login", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(respBody)
	return bodyString, nil
}

type depositStruct struct {
	acctNum    string
	routingNum string
	amount     int
}

func depositReq(client http.Client, port string, deposit depositStruct) (string, error) {
	params := url.Values{}
	params.Add("account", `add`)
	params.Add("external_account_num", deposit.acctNum)
	params.Add("external_routing_num", deposit.routingNum)
	params.Add("external_label", ``)
	params.Add("amount", strconv.Itoa(deposit.amount))

	id := uuid.New()
	params.Add("uuid", id.String())

	body := strings.NewReader(params.Encode())

	req, err := http.NewRequest("POST", "http://localhost:"+port+"/deposit", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(respBody)
	return bodyString, nil
}

type sendStruct struct {
	acctNum    string
	routingNum string
	amount     int
}

func sendReq(client http.Client, port string, send sendStruct) (string, error) {
	params := url.Values{}
	params.Add("account_num", `add`)
	params.Add("contact_account_num", send.acctNum)
	params.Add("external_routing_num", send.routingNum)
	params.Add("contact_label", ``)
	params.Add("amount", strconv.Itoa(send.amount))

	id := uuid.New()
	params.Add("uuid", id.String())

	body := strings.NewReader(params.Encode())

	req, err := http.NewRequest("POST", "http://localhost:"+port+"/payment", body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	bodyString := string(respBody)
	return bodyString, nil
}

type listOfReqs func() []interface{}

func listOfReqs1() []interface{} {
	args := []interface{}{}
	username := "test"
	password := "1234"

	// signup
	signUp := signUpStruct{
		username:  username,
		password:  password,
		firstname: "test",
		lastname:  "test",
	}
	args = append(args, signUp)

	// login
	login := loginStruct{
		username: username,
		password: password,
	}
	args = append(args, login)

	// deposit
	deposit := depositStruct{
		acctNum:    "1234567890",
		routingNum: "123456789",
		amount:     1800,
	}
	args = append(args, deposit)

	// send
	send := sendStruct{
		acctNum:    "9876543210",
		routingNum: "987654321",
		amount:     1000,
	}
	args = append(args, send)

	// send
	send2 := sendStruct{
		acctNum:    "9876543210",
		routingNum: "987654321",
		amount:     800,
	}
	args = append(args, send2)

	return args
}

func req(client http.Client, port string, arg interface{}) (string, error) {
	switch req := arg.(type) {
	case signUpStruct:
		output, err := signupReq(client, port, req)
		if err != nil {
			fmt.Printf("signup req failed: %v\n", err)
		}
		return output, err
	case loginStruct:
		output, err := loginReq(client, port, req)
		if err != nil {
			fmt.Printf("login req failed: %v\n", err)
		}
		return output, err
	case depositStruct:
		output, err := depositReq(client, port, req)
		if err != nil {
			fmt.Printf("deposit req failed: %v\n", err)
		}
		return output, err
	case sendStruct:
		output, err := sendReq(client, port, req)
		if err != nil {
			fmt.Printf("send req failed: %v\n", err)
		}
		return output, err
	default:
		return "", fmt.Errorf("unknown req struct: %+v", arg)
	}
}
