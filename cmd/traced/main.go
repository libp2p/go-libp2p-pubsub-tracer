package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"

	libp2p "github.com/libp2p/go-libp2p"
	pnet "github.com/libp2p/go-libp2p-pnet"
	tracer "github.com/libp2p/go-libp2p-pubsub-tracer"
)

func main() {
	port := flag.Int("port", 4001, "port to listen to")
	id := flag.String("id", "identity", "daemon identity file")
	dir := flag.String("dir", "traced.out", "trace log directory")
	flag.Parse()

	var privkey crypto.PrivKey

	if _, err := os.Stat(*id); err == nil {
		privkey, err = readIdentity(*id)
		if err != nil {
			log.Fatal(err)
		}
	} else if os.IsNotExist(err) {
		log.Printf("Generating peer identity in %s", *id)
		privkey, err = generateIdentity(*id)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal(err)
	}

	pid, err := peer.IDFromPublicKey(privkey.GetPublic())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("I am %s\n", pid)

	var opts []libp2p.Option

	opts = append(opts,
		libp2p.Identity(privkey),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *port)),
	)

	if pnk := os.Getenv("PNET_KEY"); pnk != "" {
		protector, err := pnet.NewProtector(strings.NewReader(pnk))
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, libp2p.PrivateNetwork(protector))
	}

	host, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		log.Fatal(err)
	}

	tr, err := tracer.NewTracer(host, *dir)
	if err != nil {
		log.Fatal(err)
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGHUP, syscall.SIGTERM)

	for {
		s := <-sigch
		switch s {
		case syscall.SIGINT, syscall.SIGTERM:
			tr.Stop()
			os.Exit(0)

		case syscall.SIGHUP:
			tr.Flush()
		}
	}
}

func readIdentity(path string) (crypto.PrivKey, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return crypto.UnmarshalPrivateKey(bytes)
}

func generateIdentity(path string) (crypto.PrivKey, error) {
	priv, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, err
	}

	bytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	err = ioutil.WriteFile(path, bytes, 0400)

	return priv, err
}
