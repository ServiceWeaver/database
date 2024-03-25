package utility

import "strings"

func GetProdDbNameBySnapshot(name string) string {
	n := strings.LastIndex(name, "snapshot")
	return name[:n]
}

func GetSnapshotDbNameByProd(name string) string {
	return name + "snapshot"
}
