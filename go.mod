module github.com/tawalaya/corral_plus_tpch

go 1.15

require (
	github.com/ISE-SMILE/corral v0.1.1
	github.com/alvaroloes/enumer v1.1.2 // indirect
	github.com/google/martian/v3 v3.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.8.1
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/tools v0.1.5 // indirect
)

//Local Stuff
replace github.com/ISE-SMILE/corral v0.1.1 => ../corral

replace github.com/mittwald/go-helm-client v0.8.0 => github.com/tawalaya/go-helm-client v0.8.1-0.20210712123422-3ceb0a361005

replace helm.sh/helm/v3 v3.6.2 => github.com/tawalaya/helm/v3 v3.6.1-0.20210712122657-0c8e3e9a7eb4
