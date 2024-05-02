package balancereader

import (
	"errors"
	"strings"

	"github.com/ServiceWeaver/weaver"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Balance struct {
	weaver.AutoMarshal
	Acctid string `gorm:"not null"`
	Amount int64  `gorm:"not null"`
}

type balanceDB struct {
	db *gorm.DB
}

func newBalanceDB(uri string) (*balanceDB, error) {
	db, err := gorm.Open(postgres.Open(uri))
	if err != nil {
		return nil, err
	}
	return &balanceDB{db: db}, nil
}

func (b *balanceDB) getBalance(acctid string) (*Balance, error) {
	var balance Balance
	updatedAcctId := strings.TrimPrefix(acctid, "00")

	if len(acctid) == 10 {
		updatedAcctId = updatedAcctId + "  "
	}
	err := b.db.Table("balances").Where("acctid = ?", updatedAcctId).Find(&balance).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &Balance{Acctid: acctid, Amount: 0}, nil
	}
	if err != nil {
		return nil, err
	}
	return &balance, nil
}
