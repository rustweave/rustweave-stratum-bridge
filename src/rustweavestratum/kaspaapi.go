package rustweavestratum

import (
	"context"
	"fmt"
	"time"

	"github.com/rustweave-network/rustweaved/app/appmessage"
	"github.com/rustweave-network/rustweaved/infrastructure/network/rpcclient"
	"github.com/rustweave/rustweave-stratum-bridge/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type rustweaveApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	rustweaved        *rpcclient.RPCClient
	connected     bool
}

func NewrustweaveAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger) (*rustweaveApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &rustweaveApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "rustweaveapi:"+address)),
		rustweaved:        client,
		connected:     true,
	}, nil
}

func (ks *rustweaveApi) Start(ctx context.Context, blockCb func()) {
	ks.waitForSync(true)
	go ks.startBlockTemplateListener(ctx, blockCb)
	go ks.startStatsThread(ctx)
}

func (ks *rustweaveApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ks.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := ks.rustweaved.GetBlockDAGInfo()
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from rustweave, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := ks.rustweaved.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from rustweave, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (ks *rustweaveApi) reconnect() error {
	if ks.rustweaved != nil {
		return ks.rustweaved.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(ks.address)
	if err != nil {
		return err
	}
	ks.rustweaved = client
	return nil
}

func (s *rustweaveApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking rustweaved sync state")
	}
	for {
		clientInfo, err := s.rustweaved.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from rustweaved @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("rustweave is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("rustweaved synced, starting server")
	}
	return nil
}

func (s *rustweaveApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := s.rustweaved.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		blockReadyChan <- true
	})
	if err != nil {
		s.logger.Error("fatal: failed to register for block notifications from rustweave")
	}

	ticker := time.NewTicker(s.blockWaitTime)
	for {
		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking rustweaved sync state, attempting reconnect: ", err)
			if err := s.reconnect(); err != nil {
				s.logger.Error("error reconnecting to rustweaved, waiting before retry: ", err)
				time.Sleep(5 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (ks *rustweaveApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	template, err := ks.rustweaved.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`'%s' via onemorebsmith/rustweave-stratum-bridge_%s`, client.RemoteApp, version))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from rustweave")
	}
	return template, nil
}
