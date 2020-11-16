

ftp-server:
	@docker run -d --name ftpd_server -p 21:21 -p 30000-30099:30000-30099 \
	-e PUBLICHOST=localhost -e FTP_MAX_CLIENTS=50 -e FTP_MAX_CONNECTIONS=50 \
	-e FTP_PASSIVE_PORTS=30000:30099 \
	-e FTP_USER_NAME=bob -e FTP_USER_PASS=12345 -e FTP_USER_HOME=/home/bob \
	stilliard/pure-ftpd
