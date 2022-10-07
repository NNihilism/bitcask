FROM golang:latest

# 配置模块代理
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct

RUN mkdir /app

WORKDIR /app

ADD . /app
 
 # 打包 AMD64 架构
# RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s' -o go_server

RUN go build -o server ./cmd/server/main.go

EXPOSE 55201

CMD ./server