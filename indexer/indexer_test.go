package indexer

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestIndexCreation(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
	})
}

// func TestAddMapping(t *testing.T) {
// 	indexPath := "./test.index"

// 	Convey("Create a new index at "+indexPath, t, func() {
// 		index := NewDefaultIndexer(indexPath)
// 		defer index.Close()
// 		defer os.RemoveAll(indexPath)
// 		Convey("Add mapping", func() {
// 			index.AddStructMapping("food")
// 		})
// 	})
// }

func TestIndexDocument(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}
				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}
			})
		})
	})
}

func TestIndexQuery(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}

				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}
				Convey("Query document", func() {
					res, err := index.Query("white")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "yam")
				})
			})
		})
	})
}

func TestIndexQueryField(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}

				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}
				Convey("Query document", func() {
					res, err := index.MatchQuery("white", "Description")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "yam")
				})
			})
		})
	})
}

func TestIndexMultipleObjects(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}

				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}

				user := struct {
					Name  string
					Email string
				}{
					Name:  "osiloke",
					Email: "osi@emoekpere.org",
				}

				err = index.IndexDocument(user.Name, user)
				if err != nil {
					panic(err)
				}
				Convey("Query document", func() {
					res, err := index.MatchQuery("osi@emoekpere.org", "Email")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "osiloke")
				})
			})
		})
	})
}

func TestIndexer_FacetedSearch(t *testing.T) {
	indexPath := "./test.index"
	os.RemoveAll(indexPath)

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				foods := []struct {
					Name        string `json:"name"`
					Description string `json:"description"`
					Type        string `json:"type"`
				}{
					{
						Name:        "egg",
						Type:        "protein",
						Description: "An egg.",
					},
					{
						Name:        "apple",
						Type:        "fruit",
						Description: "An egg.",
					},
					{
						Name:        "broccoli",
						Type:        "protein",
						Description: "A green edible protein",
					},
					{
						Name:        "plantain",
						Type:        "carb",
						Description: "A starchy stuff.",
					},
					{
						Name:        "yam",
						Type:        "carb",
						Description: "A white vegetable thats actually starchy.",
					},
					{
						Name:        "potato",
						Type:        "carb",
						Description: "A white vegetable thats actually starchy.",
					},
				}
				for _, food := range foods {
					err := index.IndexDocument(food.Name, food)
					if err != nil {
						panic(err)
					}
				}
				Convey("Faceted query search", func() {
					res, err := index.FacetedQuery("*", &Facets{
						Top: map[string]TopFacet{
							"TopTypes": {"TopTypes", "type", 2},
						},
					}, 1, 0, true, []string{"*"})
					if err != nil {
						panic(err)
					}

					So(res.Facets["TopTypes"].Terms[0].Term, ShouldEqual, "carb")
					So(res.Facets["TopTypes"].Terms[1].Term, ShouldEqual, "protein")
				})
			})
		})
	})
}
