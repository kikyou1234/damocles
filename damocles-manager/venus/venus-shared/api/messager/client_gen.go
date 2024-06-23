// Code generated by github.com/filecoin-project/venus/venus-devtool/api-gen. DO NOT EDIT.
package messager

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/filecoin-project/venus/venus-shared/api"
)

const MajorVersion = 0
const APINamespace = "messager.IMessager"
const MethodNamespace = "Message"

// NewIMessagerRPC creates a new httpparse jsonrpc remotecli.
func NewIMessagerRPC(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (IMessager, jsonrpc.ClientCloser, error) {
	endpoint, err := api.Endpoint(addr, MajorVersion)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid addr %s: %w", addr, err)
	}

	if requestHeader == nil {
		requestHeader = http.Header{}
	}
	requestHeader.Set(api.VenusAPINamespaceHeader, APINamespace)

	var res IMessagerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, endpoint, MethodNamespace, api.GetInternalStructs(&res), requestHeader, opts...)

	return &res, closer, err
}

// DialIMessagerRPC is a more convinient way of building client, as it resolves any format (url, multiaddr) of addr string.
func DialIMessagerRPC(ctx context.Context, addr string, token string, requestHeader http.Header, opts ...jsonrpc.Option) (IMessager, jsonrpc.ClientCloser, error) {
	ainfo := api.NewAPIInfo(addr, token)
	endpoint, err := ainfo.DialArgs(api.VerString(MajorVersion))
	if err != nil {
		return nil, nil, fmt.Errorf("get dial args: %w", err)
	}

	if requestHeader == nil {
		requestHeader = http.Header{}
	}
	requestHeader.Set(api.VenusAPINamespaceHeader, APINamespace)
	ainfo.SetAuthHeader(requestHeader)

	var res IMessagerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, endpoint, MethodNamespace, api.GetInternalStructs(&res), requestHeader, opts...)

	return &res, closer, err
}
