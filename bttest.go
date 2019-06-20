package bttest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"cloud.google.com/go/bigtable"
	bt "cloud.google.com/go/bigtable"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"golang.org/x/sync/errgroup"
)

const (
	FAM  = "tst"
	QUAL = "time"
)

var (
	writeDelay = time.Second * 5
	readDelay  = time.Millisecond * 100
	errGet     = fmt.Errorf("Failed to get time")
)

type tester struct {
	clients  map[string]*bt.Client
	profiles []string
}

func NewTester(ctx context.Context, cfg *Config) (*tester, error) {
	t := &tester{profiles: cfg.AppProfiles}

	clients := make(map[string]*bt.Client)

	for _, profile := range cfg.AppProfiles {
		log.WithFields(log.Fields{
			"profile":  profile,
			"instance": cfg.Instance,
			"project":  cfg.Project,
		}).Info("Connecting with profile")
		btcfg := bt.ClientConfig{
			AppProfile: profile,
		}
		client, err := bt.NewClientWithConfig(ctx, cfg.Project, cfg.Instance, btcfg)

		if err != nil {
			return nil, err
		}
		clients[profile] = client
	}
	t.clients = clients
	return t, nil
}

func (t *tester) Close() error {
	return nil
}

func (t *tester) Run(ctx context.Context) {
	group, ctx := errgroup.WithContext(ctx)

	// Create readers
	for myProfile, client := range t.clients {
		// shadow variables so we don't leak
		myProfile := myProfile
		client := client

		group.Go(func() error { return t.reader(ctx, client, myProfile) })
	}

	// Create a writer
	group.Go(func() error { return t.writer(ctx, group) })
}

func (t *tester) reader(ctx context.Context, client *bt.Client, myProfile string) error {
	clt := client.Open("test")
	delayMap := make(map[string]time.Time)
	for {
		for _, profile := range t.profiles {
			row, err := clt.ReadRow(ctx, profile, bigtable.RowFilter(bigtable.ColumnFilter(QUAL)))
			if err != nil {
				if err == context.Canceled {
					return err
				}
				log.WithError(err).Println("failed to get row: ", myProfile)
				continue
			}

			// there may be a better way to avoid clock skew, but i can't think of it
			now := time.Now()
			ptime, err := getWriteTime(row)
			if err != nil {
				if err == context.Canceled {
					return err
				}
				continue
			}
			if d, ok := delayMap[profile]; ok && d != ptime {
				log.WithFields(log.Fields{
					"read-profile":  myProfile,
					"write-profile": profile,
					"delay":         now.Sub(ptime),
				}).Info("read time")
			}
			delayMap[profile] = ptime
		}
		// Don't hammer BT too hard.
		<-time.After(readDelay)
	}

}

func (t *tester) writer(ctx context.Context, group *errgroup.Group) error {
	for {
		for _, client := range t.clients {
			for _, profile := range t.profiles {
				group.Go(func() error {
					tbl := client.Open("test")
					mut := bigtable.NewMutation()
					setWriteTime(mut, time.Now())

					err := tbl.Apply(ctx, profile, mut)
					log.WithField("profile", profile).Info("writing")

					return err
				})

				select {
				case <-time.After(writeDelay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}

}

func getWriteTime(row bt.Row) (time.Time, error) {
	if len(row[FAM]) == 0 {
		return time.Time{}, errGet
	}
	ts := time.Duration(int64(row[FAM][0].Timestamp)) //bigtable uses microseconds, and timestamp is an int64

	t := time.Unix(0, 0)
	t = t.Add(time.Microsecond * ts)
	return t, nil

}

func getQualTimeValue(row bt.Row) (time.Time, error) {
	var value int64
	if len(row[FAM]) == 0 {
		return time.Time{}, errGet
	}
	raw := row[FAM][0].Value

	buf := bytes.NewReader(raw)
	err := binary.Read(buf, binary.BigEndian, &value)
	if err != nil {
		log.WithError(err).Info("found time")
		return time.Time{}, err
	}

	return time.Unix(value, 0), nil
}

func setWriteTime(mut *bigtable.Mutation, value time.Time) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, value.Unix())
	if err != nil {
		panic(errors.Wrapf(err, "failed to write time"))
	}
	mut.Set(FAM, QUAL, bigtable.ServerTime, buf.Bytes())
}
