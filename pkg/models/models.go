package models

type Customer struct {
	Name string
}

type Order struct {
	Product string
	Price   float64
	Client  string
}

type Payment struct {
	Amount int
}
