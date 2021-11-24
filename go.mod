module github.com/tawalaya/corral_plus_tpch

go 1.15

require (
	github.com/ISE-SMILE/corral v0.1.3
	github.com/go-git/go-git/v5 v5.4.2
	github.com/google/martian/v3 v3.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.8.1
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
)

//Local Stuff
replace github.com/ISE-SMILE/corral v0.1.3 => ../corral
