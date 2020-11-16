
funnel:
	@curl -L -O https://github.com/ohsu-comp-bio/funnel/releases/download/0.10.0/funnel-linux-amd64-0.10.0.tar.gz
	@tar xvzf funnel-linux-amd64-0.10.0.tar.gz funnel

funnel-server: funnel
	@./funnel server run --Logger.OutputFile funnel.logs &

ftp-server:
	@docker run -d --name ftpd_server -p 21:21 -p 30000-30099:30000-30099 \
	-e PUBLICHOST=localhost -e FTP_MAX_CLIENTS=50 -e FTP_MAX_CONNECTIONS=50 \
	-e FTP_PASSIVE_PORTS=30000:30099 \
	-e FTP_USER_NAME=bob -e FTP_USER_PASS=12345 -e FTP_USER_HOME=/home/bob \
	stilliard/pure-ftpd

ftp-test:
	@./cwl-tes --tes http://localhost:8000 --insecure \
	--remote-storage-url ftp://bob:12345@localhost \
	tests/hashsplitter-workflow.cwl.yml tests/input.json
