package db

func GetRouter()map[string]CmdFunc{
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = Ping


	routerMap["set"] = Set
	routerMap["get"] = Get

	return routerMap
}
