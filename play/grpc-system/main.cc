#include <grpcpp/grpcpp.h>
#include "foo.grpc.pb.h"

class FooServiceImpl final : public small::gossip::FooService::Service {
    grpc::Status Exchange(grpc::ServerContext*,
                          const small::gossip::Entries*,
                          small::gossip::Entries*) override {
        return grpc::Status::OK;
    }
};

int main() {
    FooServiceImpl service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "gRPC server listening on 0.0.0.0:50051\n";
    server->Wait();
    return 0;
}