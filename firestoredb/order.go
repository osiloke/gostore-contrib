package firestoredb

import "cloud.google.com/go/firestore"

//
// order = indexer.OrderRequest([]string{"-_score", "-_id"})
func OrderQuery(order []string, q *firestore.CollectionRef) *firestore.CollectionRef {
	for _, o := range order {
		valRune := []rune(o)
		var first rune
		if len(valRune) > 0 {
			first = valRune[0]
		} else {
			first = 0
		}
		field := string(valRune[1:])
		if string(first) == "+" {
			q.Query = q.Query.OrderBy(field, firestore.Asc)
		} else {
			q.Query = q.Query.OrderBy(field, firestore.Desc)
		}
	}
	return q
}
