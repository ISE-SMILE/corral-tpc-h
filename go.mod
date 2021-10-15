module github.com/tawalaya/corral_plus_tpch

go 1.15

require (
	github.com/ISE-SMILE/corral v0.1.1
	github.com/go-git/go-git v4.7.0+incompatible
	github.com/go-git/go-git/v5 v5.4.2
	github.com/google/martian/v3 v3.1.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.8.1
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	gopkg.in/src-d/go-git.v4 v4.13.1 // indirect
)

//Local Stuff
replace github.com/ISE-SMILE/corral v0.1.1 => ../corral

replace github.com/mittwald/go-helm-client v0.8.0 => github.com/tawalaya/go-helm-client v0.8.1-0.20210712123422-3ceb0a361005

replace helm.sh/helm/v3 v3.6.2 => github.com/tawalaya/helm/v3 v3.6.1-0.20210712122657-0c8e3e9a7eb4
