package utility

func GetProdDbNameBySnapshot(name string) string {
	return name[:len(name)-len("snapshot")]
}

func GetSnapshotDbNameByProd(name string) string {
	return name + "snapshot"
}
