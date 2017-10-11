// Copyright © 2017 Osiloke Emoekpere <me@osiloke.com>
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

package cmd

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/osiloke/gostore"
	badgerdb "github.com/osiloke/gostore-contrib/badger"
	boltstore "github.com/osiloke/gostore-contrib/bolt"

	"github.com/spf13/cobra"
)

var (
	path, name, action, data, key, store string
	count                                int
)

func getStore(name, path string) (gostore.ObjectStore, error) {
	switch name {
	case "BADGER":
		return badgerdb.New(path)
	case "BOLT":
		return boltstore.New(path)
	}
	return nil, errors.New("No store named " + name)
}

// dbCmd represents the db command
var dbCmd = &cobra.Command{
	Use:   "db",
	Short: "Execute db commands",
	Long:  `Execute db commands.`,
	Run: func(cmd *cobra.Command, args []string) {

		db, err := getStore(name, path)
		if err != nil {
			panic(err.Error())
		}
		defer db.Close()

		switch action {
		case "getAll":
			rows, err := db.All(count, 0, store)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		OUTER:
			for {
				var d map[string]interface{}
				ok, err := rows.Next(&d)
				if !ok {
					if err != nil {
						println(err.Error())
					}
					break OUTER
				}
				row, err := json.MarshalIndent(&d, "", "    ")
				if err != nil {
					continue
				}
				fmt.Println(string(row))
			}
			rows.Close()
		case "get":
			_data := make(map[string]interface{})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			err = db.Get(key, store, &_data)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println(fmt.Sprintf("%s = %v", key, _data))
		case "create":
			_data := make(map[string]interface{})
			err = json.Unmarshal([]byte(data), _data)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			var _k string
			_k, err = db.Save(store, &data)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println(_k + " created")
		case "update":
		case "delete":

		}

	},
}

func init() {
	RootCmd.AddCommand(dbCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dbCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dbCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	dbCmd.Flags().StringVarP(&path, "path", "p", "./db", "path to gostore data folder")
	dbCmd.Flags().StringVarP(&name, "type", "t", "BADGER", "type of gostore")
	dbCmd.Flags().StringVarP(&action, "action", "a", "get", "action to perform, get, save, update")
	dbCmd.Flags().StringVarP(&key, "key", "k", "", "key to operate on")
	dbCmd.Flags().StringVarP(&store, "store", "s", "_test", "store")
	dbCmd.Flags().IntVarP(&count, "count", "c", 100, "count of rows to return")
}
