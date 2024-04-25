// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledgerwriter

import (
	"bankofanthos_prototype/bankofanthos/balancereader"
	"bankofanthos_prototype/bankofanthos/model"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ServiceWeaver/weaver"

	"github.com/patrickmn/go-cache"
)

type T interface {
	// AddTransaction records a new transaction in the transaction database.
	AddTransaction(ctx context.Context, requestUuid, authenticatedAccount string, transaction model.Transaction) error
}

type config struct {
	LocalRoutingNum string `toml:"local_routing_num"`
	DataSourceURL   string `toml:"data_source_url"`
	AccountIdLength int    `toml:"account_id_length"`
}

type impl struct {
	weaver.Implements[T]
	weaver.WithConfig[config]
	cache         *cache.Cache
	txnRepo       *transactionRepository
	balanceReader weaver.Ref[balancereader.T]
}

func (i *impl) Init(context.Context) error {
	var err error
	i.txnRepo, err = newTransactionRepository(i.Config().DataSourceURL)
	if err != nil {
		return err
	}
	i.cache = cache.New(1*time.Hour, 1*time.Minute)
	return nil
}

// getAvailableBalance calls the balance component to fetch accountNum's balance.
func (i *impl) getAvailableBalance(ctx context.Context, accountNum string) (int64, error) {
	return i.balanceReader.Get().GetBalance(ctx, accountNum)
}

// AddTransaction implements the T interface.
// Note: there may be some bugs while adding transactions because original bank of anthos has bugs for handling it.
func (i *impl) AddTransaction(ctx context.Context, requestUuid, authenticatedAccount string, transaction model.Transaction) error {
	// Check for duplicate transactions.
	if _, ok := i.cache.Get(requestUuid); ok {
		return fmt.Errorf("duplicate transaction")
	}

	// Validate transaction.
	err := validateTransaction(i.Config().LocalRoutingNum, authenticatedAccount, &transaction, i.Config().AccountIdLength)
	if err != nil {
		return err
	}

	// Ensure sender balance can cover the transaction.
	if transaction.FromRoutingNum == i.Config().LocalRoutingNum {
		balance, err := i.getAvailableBalance(ctx, transaction.FromAccountNum)
		if err != nil {
			err = errors.New("failed to retrieve account balance")
			return err
		}
		if balance < transaction.Amount {
			return fmt.Errorf("transaction submission failed: Insufficient balance %d", balance)
		}

		updatedAmount := balance - int64(transaction.Amount)
		acctId := strings.TrimPrefix(transaction.FromAccountNum, "00")
		if len(acctId) == 10 {
			acctId = acctId + "  "
		}
		err = i.txnRepo.updateBalance(acctId, updatedAmount)
		if err != nil {
			return err
		}
	}

	if transaction.ToRoutingNum == i.Config().LocalRoutingNum {
		balance, err := i.getAvailableBalance(ctx, transaction.ToAccountNum)
		if err != nil {
			return err
		}

		updatedAmount := balance + int64(transaction.Amount)
		acctId := strings.TrimPrefix(transaction.ToAccountNum, "00")
		if len(acctId) == 10 {
			acctId = acctId + "  "
		}
		err = i.txnRepo.updateBalance(acctId, updatedAmount)
		if err != nil {
			return err
		}
	}

	// Save transaction to ledger database as well as to the cache.
	err = i.txnRepo.save(&transaction)
	if err != nil {
		return err
	}
	i.cache.Set(requestUuid, transaction, 0)
	return nil
}

// Account IDs should be 10 digits between 0 and 9.
var acctRegex = regexp.MustCompile("^[0-9]{10}$")

// Route numbers should be 9 digits between 0 and 9.
var routeRegex = regexp.MustCompile("^[0-9]{9}$")

// validateTransaction ensures that a transaction is valid before it is added to the ledger.
func validateTransaction(localRoutingNum, authedAcct string, t *model.Transaction, accountIdLength int) error {
	// Validate account and routing numbers.
	t.FromAccountNum = strings.TrimSpace(t.FromAccountNum)
	t.ToAccountNum = strings.TrimSpace(t.ToAccountNum)

	originalFromAccountNum := t.FromAccountNum
	// [BUG]backward compatible with baseline
	if accountIdLength == 12 {
		acctRegex = regexp.MustCompile("^[0-9]{12}$")
		if len(t.FromAccountNum) == 10 {
			t.FromAccountNum = "00" + t.FromAccountNum
		}
		if len(t.ToAccountNum) == 10 {
			t.ToAccountNum = "00" + t.ToAccountNum
		}
	}
	// end of [BUG]

	if !acctRegex.MatchString(t.FromAccountNum) || !acctRegex.MatchString(t.ToAccountNum) {
		return fmt.Errorf("invalid transaction: Invalid account details: %s %s", t.FromAccountNum, t.ToAccountNum)
	}

	if !routeRegex.MatchString(t.FromRoutingNum) || !routeRegex.MatchString(t.ToRoutingNum) {
		return fmt.Errorf("invalid transaction: Invalid account routing details: %s %s", t.FromRoutingNum, t.ToRoutingNum)
	}
	// [BUG]
	// If this is an internal transaction, ensure it originated from the authenticated user.
	if t.FromRoutingNum == localRoutingNum && strings.TrimSpace(originalFromAccountNum) != strings.TrimSpace(authedAcct) {
		return fmt.Errorf("invalid transaction: Sender not authorized [%s][%s]", strings.TrimSpace(t.FromAccountNum), strings.TrimSpace(authedAcct))
	}
	// end of [BUG]

	// Ensure sender isn't the receiver.
	if t.FromAccountNum == t.ToAccountNum && t.FromRoutingNum == t.ToRoutingNum {
		return fmt.Errorf("invalid transaction: Sender is also the receiver")
	}

	// Ensure that the amount is valid.
	if t.Amount <= 0 {
		return fmt.Errorf("invalid transaction: Transaction amount (%d) is invalid", t.Amount)
	}
	return nil
}
