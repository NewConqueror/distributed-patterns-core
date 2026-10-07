package concurrency

import (
	"sort"
)

// OrderAccountIDs, verilen hesap ID'lerini tekilleştirir ve deterministik olarak
// küçükten büyüğe (alfabetik/leksikografik) sıralar.
//
// Deadlock Önleme Prensibi:
// İki veya daha fazla eşzamanlı işlem (örneğin A -> B ve B -> A transferleri)
// kilitleri her zaman AYNI GLOBAL SIRADA (A sonra B) talep ederse,
// döngüsel bekleme (circular wait) koşulu matematiksel olarak imkansız hale gelir.
func OrderAccountIDs(accountIDs []string) []string {
	if len(accountIDs) == 0 {
		return nil
	}

	unique := make(map[string]struct{}, len(accountIDs))
	for _, id := range accountIDs {
		if id != "" {
			unique[id] = struct{}{}
		}
	}

	ordered := make([]string, 0, len(unique))
	for id := range unique {
		ordered = append(ordered, id)
	}

	sort.Strings(ordered)
	return ordered
}
