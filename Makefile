env:
	echo "openwhisk/action-golang-v1.15" > exec.env

build:
	CGO_ENABLED=0 go build -o exec -ldflags "-s -w" .
	
zip:
	zip exec.zip exec exec.env
	rm exec 
	rm exec.env

whiskBuild:
	GOARCH=amd64 GOOS=linux CGO_ENABLED=0 go build -o exec -ldflags "-s -w" .

clean:
	rm exec.zip
	rm exec
	rm exec.env
	rm package.json

package: env build zip
	echo "{\"value\":{\"code\":\"\c" > payload.json
	gbase64 -w 0 exec.zip >> payload.json
	echo "\",\"Binary\":true}}\c" >> payload.json

localInit: package
	curl -i -X POST -H "Content-Type: application/json" -d @payload.json http://localhost:8080/init
	rm payload.json

localRun: 
	curl -i -X POST -H "Content-Type: application/json" -d @req.json http://localhost:8080/run