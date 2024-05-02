package utility

const totalCnt = 4

type Trail struct {
	Name         string
	ReqPorts     []string
	Cnt          int
	ProdServices []*ProdService
}

func GetTrials(reqCnt int, v1Port, v2Port, origListenPort string) []*Trail {
	var trails []*Trail
	name := ""
	for r := 0; r < totalCnt; r++ {
		ports := []string{}
		if r == 0 {
			// for all v1 traffic
			for i := 0; i < reqCnt; i++ {
				ports = append(ports, v1Port)
			}
			name = "Control"
		}

		if r == 1 {
			// for all v2 traffic
			for i := 0; i < reqCnt; i++ {
				ports = append(ports, v2Port)
			}
			name = "E_C"
		}

		if r == 2 {
			// half to v1, half to v2
			for i := 0; i < reqCnt/2; i++ {
				ports = append(ports, v1Port)
			}
			for i := reqCnt / 2; i < reqCnt; i++ {
				ports = append(ports, v2Port)
			}
			name = "E_SC"
		}

		if r == 3 {
			// half to v2, half to v1
			for i := 0; i < reqCnt/2; i++ {
				ports = append(ports, v2Port)
			}
			for i := reqCnt / 2; i < reqCnt; i++ {
				ports = append(ports, v1Port)
			}
			name = "E_CS"
		}

		trial := &Trail{
			ReqPorts: ports,
			Cnt:      r,
			Name:     name,
		}
		trails = append(trails, trial)
	}

	return trails
}

func (t *Trail) IsControl() bool {
	return t.Name == "Control"
}

func (t *Trail) IsConaryOnly() bool {
	return t.Name == "E_C"
}
