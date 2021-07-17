module github.com/tawalaya/corral_plus_tpch

go 1.15

require (
	github.com/ISE-SMILE/corral v0.1.1
	github.com/google/martian/v3 v3.1.0
	github.com/spf13/viper v1.8.1
)

//Local Stuff
replace github.com/ISE-SMILE/corral v0.1.1 => ../corral

replace github.com/mittwald/go-helm-client v0.8.0 => github.com/tawalaya/go-helm-client v0.8.1-0.20210712123422-3ceb0a361005

replace helm.sh/helm/v3 v3.6.2 => github.com/tawalaya/helm/v3 v3.6.1-0.20210712122657-0c8e3e9a7eb4
