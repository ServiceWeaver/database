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

package balancereader

import cache "github.com/goburrow/cache"

type balanceCache struct {
	c cache.LoadingCache
}

func newBalanceCache(db *balanceDB, expireSize int) *balanceCache {
	load := func(accountID cache.Key) (cache.Value, error) {
		balance, err := db.getBalance(accountID.(string))
		if err != nil {
			return nil, err
		}
		return balance.Amount, nil
	}
	return &balanceCache{
		c: cache.NewLoadingCache(
			load,
			cache.WithMaximumSize(expireSize),
		),
	}
}
