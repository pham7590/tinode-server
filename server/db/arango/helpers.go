package arango

import (
	"errors"
	"fmt"

	driver "github.com/arangodb/go-driver"
)

func (a *adapter) QueryOnef(
	queryString string,
	answer interface{},
	args ...interface{},
) error {
	if a.db == nil {
		return errors.New("adapter arangodb not connected")
	}
	formatted := fmt.Sprintf(queryString, args...)
	//log.Println("##############")
	//log.Println(formatted)
	//log.Println("##############")
	cursor, err := a.db.Query(a.ctx,
		formatted,
		nil,
	)
	if err != nil {
		return err
	}
	defer cursor.Close()
	_, err = cursor.ReadDocument(a.ctx, answer)
	if err != nil {
		return err
	}
	return nil
}

func (a *adapter) QueryManyf(
	queryString string,
	args ...interface{},
) (driver.Cursor, error) {
	if a.db == nil {
		return nil, errors.New("adapter arangodb not connected")
	}
	formatted := fmt.Sprintf(queryString, args...)
	cursor, err := a.db.Query(a.ctx,
		formatted,
		nil,
	)
	return cursor, err
}
