package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	cf, err := reader.GetCF(req.GetCf(), req.GetKey())
	return &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       cf,
		NotFound:    cf == nil,
	}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modifies := make([]storage.Modify, 1)
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}})
	err := server.storage.Write(req.GetContext(), modifies)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modifies := make([]storage.Modify, 1)
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}})
	err := server.storage.Write(req.GetContext(), modifies)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	iterCF := reader.IterCF(req.GetCf())
	defer iterCF.Close()
	pairs := make([]*kvrpcpb.KvPair, 0)
	for iterCF.Valid() {
		item := iterCF.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		})
	}
	return &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         pairs,
	}, err
}
