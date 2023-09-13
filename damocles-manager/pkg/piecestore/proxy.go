package piecestore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"

	"github.com/ipfs-force-community/damocles/damocles-manager/pkg/filestore"
	"github.com/ipfs-force-community/damocles/damocles-manager/pkg/logging"
	"github.com/ipfs-force-community/damocles/damocles-manager/pkg/market"
)

var log = logging.New("piecestore")

type PieceStore interface {
	Get(ctx context.Context, pieceCid cid.Cid) (io.ReadCloser, error)
	Put(ctx context.Context, pieceCid cid.Cid, data io.Reader) (int64, error)
}

var _ PieceStore = (*Proxy)(nil)

func NewProxy(locals []filestore.Ext, mapi market.API) *Proxy {
	return &Proxy{
		locals: locals,
		market: mapi,
	}
}

type Proxy struct {
	locals []filestore.Ext
	market market.API
}

func (p *Proxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		p.handleGet(rw, req)
	case http.MethodPut:
		p.handlePut(rw, req)
	default:
		http.Error(rw, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

func (p *Proxy) handleGet(rw http.ResponseWriter, req *http.Request) {
	path := strings.Trim(req.URL.Path, "/ ")
	c, err := cid.Decode(path)
	if err != nil {
		http.Error(rw, fmt.Sprintf("cast %s to cid: %s", path, err), http.StatusBadRequest)
		return
	}

	for _, store := range p.locals {

		_, subPath, err := store.FullPath(req.Context(), filestore.PathTypeCustom, nil, &path)
		if err != nil {
			log.Debug("get FullPath for custom(%s): %w", subPath, err)
			continue
		}

		if r, err := store.Read(req.Context(), subPath); err == nil {
			defer r.Close()
			_, err := io.Copy(rw, r)
			if err != nil {
				log.Warnw("transfer piece data for %s: %s", path, err)
			}
			return
		}
	}

	http.Redirect(rw, req, p.market.PieceResourceURL(c), http.StatusFound)
}

func (p *Proxy) handlePut(rw http.ResponseWriter, req *http.Request) {
	path := strings.Trim(req.URL.Path, "/ ")
	dataSize := req.ContentLength

	for _, store := range p.locals {
		storeInfo, err := store.InstanceInfo(req.Context())
		if err != nil {
			log.Warnw("get store instance info", "err", err)
			continue
		}

		if storeInfo.Config.GetReadOnly() {
			continue
		}

		_, subPath, err := store.FullPath(req.Context(), filestore.PathTypeCustom, nil, &path)
		if err != nil {
			log.Debug("get FullPath for custom(%s): %w", subPath, err)
			continue
		}

		// todo : we can't get the free space of the store some time, so there is compromise when free == 0
		if storeInfo.Free > uint64(dataSize) || storeInfo.Free == 0 {
			count, err := store.Write(req.Context(), subPath, req.Body)
			if err != nil {
				log.Errorw("put piece data", "path", path, "subPath", subPath, "store", storeInfo.Config.Name, "count", count, "err", err)
				http.Error(rw, fmt.Sprintf("put piece data: %s", err), http.StatusInternalServerError)
			}

			log.Infow("put piece data", "path", path, "subPath", subPath, "count", count)
			return
		}
	}
	log.Errorw("put piece data", "path", path, "err", "no store available")
	http.Error(rw, "no piece store available", http.StatusInternalServerError)
}

func (p *Proxy) Get(ctx context.Context, pieceCid cid.Cid) (io.ReadCloser, error) {
	key := pieceCid.String()
	for _, store := range p.locals {
		_, subPath, err := store.FullPath(ctx, filestore.PathTypeCustom, nil, &key)
		if err != nil {
			log.Debug("get FullPath for custom(%s): %w", key, err)
			continue
		}
		if r, err := store.Read(ctx, subPath); err == nil {
			return r, nil
		}
	}

	return nil, fmt.Errorf("not found")
}

func (p *Proxy) Put(ctx context.Context, pieceCid cid.Cid, data io.Reader) (int64, error) {
	key := pieceCid.String()
	for _, store := range p.locals {
		storeInfo, err := store.InstanceInfo(ctx)
		if err != nil {
			log.Warnw("get store instance info", "err", err)
			continue
		}

		if storeInfo.Config.GetReadOnly() {
			continue
		}

		_, subPath, err := store.FullPath(ctx, filestore.PathTypeCustom, nil, &key)
		if err != nil {
			log.Debug("get FullPath for custom(%s): %w", key, err)
			continue
		}

		count, err := store.Write(ctx, subPath, data)
		if err != nil {
			log.Errorw("put piece data", "path", key, "store", storeInfo.Config.Name, "count", count, "err", err)
			return 0, err
		}

		return count, nil
	}
	return 0, fmt.Errorf("not store available")
}
