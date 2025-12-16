#include "source/extensions/clusters/redis/redis_cluster.h"

#include <cstdint>
#include <memory>

#include "envoy/config/cluster/v3/cluster.pb.h"
#include "envoy/extensions/clusters/redis/v3/redis_cluster.pb.h"
#include "envoy/extensions/clusters/redis/v3/redis_cluster.pb.validate.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.h"
#include "envoy/extensions/filters/network/redis_proxy/v3/redis_proxy.pb.validate.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_contains.h"

namespace Envoy {
namespace Extensions {
namespace Clusters {
namespace Redis {

absl::StatusOr<std::unique_ptr<RedisCluster::RedisHost>> RedisCluster::RedisHost::create(
    Upstream::ClusterInfoConstSharedPtr cluster, const std::string& hostname,
    Network::Address::InstanceConstSharedPtr address, RedisCluster& parent, bool primary) {
  absl::Status creation_status = absl::OkStatus();
  auto ret = std::unique_ptr<RedisCluster::RedisHost>(
      new RedisCluster::RedisHost(cluster, hostname, address, parent, primary, creation_status));
  RETURN_IF_NOT_OK(creation_status);
  return ret;
}

absl::StatusOr<std::unique_ptr<RedisCluster>> RedisCluster::create(
    const envoy::config::cluster::v3::Cluster& cluster,
    const envoy::extensions::clusters::redis::v3::RedisClusterConfig& redis_cluster,
    Upstream::ClusterFactoryContext& context,
    NetworkFilters::Common::Redis::Client::ClientFactory& client_factory,
    Network::DnsResolverSharedPtr dns_resolver, ClusterSlotUpdateCallBackSharedPtr factory) {
  absl::Status creation_status = absl::OkStatus();
  std::unique_ptr<RedisCluster> ret = absl::WrapUnique(new RedisCluster(
      cluster, redis_cluster, context, client_factory, dns_resolver, factory, creation_status));
  RETURN_IF_NOT_OK(creation_status);
  return ret;
}

RedisCluster::RedisCluster(
    const envoy::config::cluster::v3::Cluster& cluster,
    const envoy::extensions::clusters::redis::v3::RedisClusterConfig& redis_cluster,
    Upstream::ClusterFactoryContext& context,
    NetworkFilters::Common::Redis::Client::ClientFactory& redis_client_factory,
    Network::DnsResolverSharedPtr dns_resolver, ClusterSlotUpdateCallBackSharedPtr lb_factory,
    absl::Status& creation_status)
    : Upstream::BaseDynamicClusterImpl(cluster, context, creation_status),
      cluster_manager_(context.serverFactoryContext().clusterManager()),
      cluster_refresh_rate_(std::chrono::milliseconds(
          PROTOBUF_GET_MS_OR_DEFAULT(redis_cluster, cluster_refresh_rate, 5000))),
      cluster_refresh_timeout_(std::chrono::milliseconds(
          PROTOBUF_GET_MS_OR_DEFAULT(redis_cluster, cluster_refresh_timeout, 3000))),
      redirect_refresh_interval_(std::chrono::milliseconds(
          PROTOBUF_GET_MS_OR_DEFAULT(redis_cluster, redirect_refresh_interval, 5000))),
      redirect_refresh_threshold_(
          PROTOBUF_GET_WRAPPED_OR_DEFAULT(redis_cluster, redirect_refresh_threshold, 5)),
      failure_refresh_threshold_(redis_cluster.failure_refresh_threshold()),
      host_degraded_refresh_threshold_(redis_cluster.host_degraded_refresh_threshold()),
      dispatcher_(context.serverFactoryContext().mainThreadDispatcher()),
      dns_resolver_(std::move(dns_resolver)),
      dns_lookup_family_(Upstream::getDnsLookupFamilyFromCluster(cluster)),
      load_assignment_(cluster.load_assignment()),
      local_info_(context.serverFactoryContext().localInfo()),
      random_(context.serverFactoryContext().api().randomGenerator()),
      redis_discovery_session_(
          std::make_shared<RedisDiscoverySession>(*this, redis_client_factory)),
      lb_factory_(std::move(lb_factory)),
      auth_username_(NetworkFilters::RedisProxy::ProtocolOptionsConfigImpl::authUsername(
          info(), context.serverFactoryContext().api())),
      auth_password_(NetworkFilters::RedisProxy::ProtocolOptionsConfigImpl::authPassword(
          info(), context.serverFactoryContext().api())),
      cluster_name_(cluster.name()), refresh_manager_(Common::Redis::getClusterRefreshManager(
                                         context.serverFactoryContext().singletonManager(),
                                         context.serverFactoryContext().mainThreadDispatcher(),
                                         context.serverFactoryContext().clusterManager(),
                                         context.serverFactoryContext().api().timeSource())),
      registration_handle_(nullptr) {
  const auto& locality_lb_endpoints = load_assignment_.endpoints();
  for (const auto& locality_lb_endpoint : locality_lb_endpoints) {
    for (const auto& lb_endpoint : locality_lb_endpoint.lb_endpoints()) {
      const auto& host = lb_endpoint.endpoint().address();
      dns_discovery_resolve_targets_.emplace_back(new DnsDiscoveryResolveTarget(
          *this, host.socket_address().address(), host.socket_address().port_value()));
    }
  }

  // Register the cluster callback using weak_ptr to avoid use-after-free
  std::weak_ptr<RedisDiscoverySession> weak_session = redis_discovery_session_;
  registration_handle_ = refresh_manager_->registerCluster(
      cluster_name_, redirect_refresh_interval_, redirect_refresh_threshold_,
      failure_refresh_threshold_, host_degraded_refresh_threshold_, [weak_session]() {
        // Try to lock the weak pointer to ensure the session is still alive
        auto session = weak_session.lock();
        if (session && session->resolve_timer_) {
          session->resolve_timer_->enableTimer(std::chrono::milliseconds(0));
        }
      });
}

RedisCluster::~RedisCluster() {
  // Set flag to prevent any callbacks from executing during destruction
  is_destroying_.store(true);

  // Reset redis_discovery_session_ before other members are destroyed
  // to ensure any pending callbacks from refresh_manager_ don't access it.
  // This matches the approach in PR #39625.
  redis_discovery_session_.reset();

  // Also clear DNS discovery targets to prevent their callbacks from
  // accessing the destroyed cluster.
  dns_discovery_resolve_targets_.clear();
}

void RedisCluster::startPreInit() {
  for (const DnsDiscoveryResolveTargetPtr& target : dns_discovery_resolve_targets_) {
    target->startResolveDns();
  }
  if (!wait_for_warm_on_init_) {
    onPreInitComplete();
  }
}

void RedisCluster::updateAllHosts(const Upstream::HostVector& hosts_added,
                                  const Upstream::HostVector& hosts_removed,
                                  uint32_t current_priority) {
  Upstream::PriorityStateManager priority_state_manager(*this, local_info_, nullptr);

  auto locality_lb_endpoint = localityLbEndpoint();
  priority_state_manager.initializePriorityFor(locality_lb_endpoint);
  for (const Upstream::HostSharedPtr& host : hosts_) {
    if (locality_lb_endpoint.priority() == current_priority) {
      priority_state_manager.registerHostForPriority(host, locality_lb_endpoint);
    }
  }

  priority_state_manager.updateClusterPrioritySet(
      current_priority, std::move(priority_state_manager.priorityState()[current_priority].first),
      hosts_added, hosts_removed, absl::nullopt, absl::nullopt, absl::nullopt);
}

void RedisCluster::onClusterSlotUpdate(ClusterSlotsSharedPtr&& slots) {
  Upstream::HostVector new_hosts;
  absl::flat_hash_set<std::string> all_new_hosts;

  for (const ClusterSlot& slot : *slots) {
    if (all_new_hosts.count(slot.primary()->asString()) == 0) {
      new_hosts.emplace_back(THROW_OR_RETURN_VALUE(
          RedisHost::create(info(), "", slot.primary(), *this, true), std::unique_ptr<RedisHost>));
      all_new_hosts.emplace(slot.primary()->asString());
    }
    for (auto const& replica : slot.replicas()) {
      if (all_new_hosts.count(replica.first) == 0) {
        new_hosts.emplace_back(
            THROW_OR_RETURN_VALUE(RedisHost::create(info(), "", replica.second, *this, false),
                                  std::unique_ptr<RedisHost>));
        all_new_hosts.emplace(replica.first);
      }
    }
  }

  // Get the map of all the latest existing hosts, which is used to filter out the existing
  // hosts in the process of updating cluster memberships.
  Upstream::HostMapConstSharedPtr all_hosts = priority_set_.crossPriorityHostMap();
  ASSERT(all_hosts != nullptr);

  Upstream::HostVector hosts_added;
  Upstream::HostVector hosts_removed;
  const bool host_updated = updateDynamicHostList(new_hosts, hosts_, hosts_added, hosts_removed,
                                                  *all_hosts, all_new_hosts);

  // Create a map containing all the latest hosts to determine whether the slots are updated.
  Upstream::HostMap updated_hosts = *all_hosts;
  for (const auto& host : hosts_removed) {
    updated_hosts.erase(host->address()->asString());
  }
  for (const auto& host : hosts_added) {
    updated_hosts[host->address()->asString()] = host;
  }

  const bool slot_updated =
      lb_factory_ ? lb_factory_->onClusterSlotUpdate(std::move(slots), updated_hosts) : false;

  // If slot is updated, call updateAllHosts regardless of if there's new hosts to force
  // update of the thread local load balancers.
  if (host_updated || slot_updated) {
    ASSERT(std::all_of(hosts_.begin(), hosts_.end(), [&](const auto& host) {
      return host->priority() == localityLbEndpoint().priority();
    }));
    updateAllHosts(hosts_added, hosts_removed, localityLbEndpoint().priority());
  } else {
    info_->configUpdateStats().update_no_rebuild_.inc();
  }

  // TODO(hyang): If there is an initialize callback, fire it now. Note that if the
  // cluster refers to multiple DNS names, this will return initialized after a single
  // DNS resolution completes. This is not perfect but is easier to code and it is unclear
  // if the extra complexity is needed so will start with this.
  onPreInitComplete();
}

void RedisCluster::reloadHealthyHostsHelper(const Upstream::HostSharedPtr& host) {
  if (lb_factory_) {
    lb_factory_->onHostHealthUpdate();
  }
  if (host && (host->coarseHealth() == Upstream::Host::Health::Degraded ||
               host->coarseHealth() == Upstream::Host::Health::Unhealthy)) {
    refresh_manager_->onHostDegraded(cluster_name_);
  }
  ClusterImplBase::reloadHealthyHostsHelper(host);
}

// DnsDiscoveryResolveTarget
RedisCluster::DnsDiscoveryResolveTarget::DnsDiscoveryResolveTarget(RedisCluster& parent,
                                                                   const std::string& dns_address,
                                                                   const uint32_t port)
    : parent_(parent), dns_address_(dns_address), port_(port) {}

RedisCluster::DnsDiscoveryResolveTarget::~DnsDiscoveryResolveTarget() {
  if (active_query_) {
    active_query_->cancel(Network::ActiveDnsQuery::CancelReason::QueryAbandoned);
  }
  // Disable timer for mock tests.
  if (resolve_timer_ && resolve_timer_->enabled()) {
    resolve_timer_->disableTimer();
  }
}

void RedisCluster::DnsDiscoveryResolveTarget::startResolveDns() {
  ENVOY_LOG(trace, "starting async DNS resolution for {}", dns_address_);

  active_query_ = parent_.dns_resolver_->resolve(
      dns_address_, parent_.dns_lookup_family_,
      [this](Network::DnsResolver::ResolutionStatus status, absl::string_view,
             std::list<Network::DnsResponse>&& response) -> void {
        active_query_ = nullptr;
        ENVOY_LOG(trace, "async DNS resolution complete for {}", dns_address_);
        if (status == Network::DnsResolver::ResolutionStatus::Failure || response.empty()) {
          if (status == Network::DnsResolver::ResolutionStatus::Failure) {
            parent_.info_->configUpdateStats().update_failure_.inc();
          } else {
            parent_.info_->configUpdateStats().update_empty_.inc();
          }

          if (!resolve_timer_) {
            resolve_timer_ = parent_.dispatcher_.createTimer([this]() -> void {
              // Check if the parent cluster is being destroyed
              if (parent_.is_destroying_.load()) {
                return;
              }
              startResolveDns();
            });
          }
          // if the initial dns resolved to empty, we'll skip the redis discovery phase and
          // treat it as an empty cluster.
          parent_.onPreInitComplete();
          resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
        } else {
          // Once the DNS resolve the initial set of addresses, call startResolveRedis on
          // the RedisDiscoverySession. The RedisDiscoverySession will using the "cluster
          // slots" command for service discovery and slot allocation. All subsequent
          // discoveries are handled by RedisDiscoverySession and will not use DNS
          // resolution again.
          parent_.redis_discovery_session_->registerDiscoveryAddress(std::move(response), port_);
          parent_.redis_discovery_session_->startResolveRedis();
        }
      });
}

// RedisCluster
RedisCluster::RedisDiscoverySession::RedisDiscoverySession(
    Envoy::Extensions::Clusters::Redis::RedisCluster& parent,
    NetworkFilters::Common::Redis::Client::ClientFactory& client_factory)
    : parent_(parent), dispatcher_(parent.dispatcher_),
      resolve_timer_(parent.dispatcher_.createTimer([this]() -> void {
        // Check if the parent cluster is being destroyed
        if (parent_.is_destroying_.load()) {
          return;
        }
        startResolveRedis();
      })),
      client_factory_(client_factory), buffer_timeout_(0),
      redis_command_stats_(
          NetworkFilters::Common::Redis::RedisCommandStats::createRedisCommandStats(
              parent_.info()->statsScope().symbolTable())) {}

// Convert the cluster slot IP/Port response to an address, return null if the response
// does not match the expected type.
Network::Address::InstanceConstSharedPtr
RedisCluster::RedisDiscoverySession::RedisDiscoverySession::ipAddressFromClusterEntry(
    const std::vector<NetworkFilters::Common::Redis::RespValue>& array) {
  return Network::Utility::parseInternetAddressNoThrow(array[0].asString(), array[1].asInteger(),
                                                       false);
}

RedisCluster::RedisDiscoverySession::~RedisDiscoverySession() {
  if (current_request_) {
    current_request_->cancel();
    current_request_ = nullptr;
  }
  // Disable timer for mock tests.
  if (resolve_timer_) {
    resolve_timer_->disableTimer();
  }

  while (!client_map_.empty()) {
    client_map_.begin()->second->client_->close();
  }
}

void RedisCluster::RedisDiscoveryClient::onEvent(Network::ConnectionEvent event) {
  if (event == Network::ConnectionEvent::RemoteClose ||
      event == Network::ConnectionEvent::LocalClose) {
    auto client_to_delete = parent_.client_map_.find(host_);
    ASSERT(client_to_delete != parent_.client_map_.end());
    parent_.dispatcher_.deferredDelete(std::move(client_to_delete->second->client_));
    parent_.client_map_.erase(client_to_delete);
  }
}

void RedisCluster::RedisDiscoverySession::registerDiscoveryAddress(
    std::list<Envoy::Network::DnsResponse>&& response, const uint32_t port) {
  // Since the address from DNS does not have port, we need to make a new address that has
  // port in it.
  for (const Network::DnsResponse& res : response) {
    const auto& addrinfo = res.addrInfo();
    ASSERT(addrinfo.address_ != nullptr);
    discovery_address_list_.push_back(
        Network::Utility::getAddressWithPort(*(addrinfo.address_), port));
  }
}

void RedisCluster::RedisDiscoverySession::startResolveRedis() {
  parent_.info_->configUpdateStats().update_attempt_.inc();
  // If a resolution is currently in progress, skip it.
  if (current_request_) {
    ENVOY_LOG(debug, "redis cluster slot request is already in progress for '{}'",
              parent_.info_->name());
    return;
  }

  // If hosts is empty, we haven't received a successful result from the CLUSTER SLOTS call
  // yet. So, pick a random discovery address from dns and make a request.
  Upstream::HostSharedPtr host;
  if (parent_.hosts_.empty()) {
    const int rand_idx = parent_.random_.random() % discovery_address_list_.size();
    auto it = std::next(discovery_address_list_.begin(), rand_idx);
    host = Upstream::HostSharedPtr{THROW_OR_RETURN_VALUE(
        RedisHost::create(parent_.info(), "", *it, parent_, true), std::unique_ptr<RedisHost>)};
  } else {
    const int rand_idx = parent_.random_.random() % parent_.hosts_.size();
    host = parent_.hosts_[rand_idx];
  }

  current_host_address_ = host->address()->asString();
  RedisDiscoveryClientPtr& client = client_map_[current_host_address_];
  if (!client) {
    client = std::make_unique<RedisDiscoveryClient>(*this);
    client->host_ = current_host_address_;
    // absl::nullopt here disables AWS IAM authentication in redis client which is not supported by
    // redis cluster implementation
    client->client_ = client_factory_.create(
        host, dispatcher_, shared_from_this(), redis_command_stats_, parent_.info()->statsScope(),
        parent_.auth_username_, parent_.auth_password_, false, absl::nullopt, absl::nullopt);
    client->client_->addConnectionCallbacks(*client);
  }
  if (Runtime::runtimeFeatureEnabled("envoy.reloadable_features.redis_use_cluster_nodes")) {
    ENVOY_LOG(debug, "executing redis cluster nodes request for '{}'", parent_.info_->name());
    current_request_ = client->client_->makeRequest(ClusterNodesRequest::instance_, *this);
  } else {
    ENVOY_LOG(debug, "executing redis cluster slots request for '{}'", parent_.info_->name());
    current_request_ = client->client_->makeRequest(ClusterSlotsRequest::instance_, *this);
  }  
}

void RedisCluster::RedisDiscoverySession::updateDnsStats(
    Network::DnsResolver::ResolutionStatus status, bool empty_response) {
  if (status == Network::DnsResolver::ResolutionStatus::Failure) {
    parent_.info_->configUpdateStats().update_failure_.inc();
  } else if (empty_response) {
    parent_.info_->configUpdateStats().update_empty_.inc();
  }
}

/**
 * Resolve the primary cluster entry hostname in each slot.
 * If the primary is successfully resolved, we proceed to resolve replicas.
 * We use the count of hostnames that require resolution to decide when the resolution process is
 * completed, and then call the post-resolution hooks.
 *
 * If resolving any one of the primary replicas fails, we stop the resolution process and reset
 * the timers to retry the resolution. Failure to resolve a replica, on the other hand does not
 * stop the process. If we replica resolution fails, we simply log a warning, and move to resolving
 * the rest.
 *
 * @param slots the list of slots which may need DNS resolution
 * @param address_resolution_required_cnt the number of hostnames that need DNS resolution
 */
void RedisCluster::RedisDiscoverySession::resolveClusterHostnames(
    ClusterSlotsSharedPtr&& slots,
    std::shared_ptr<std::uint64_t> hostname_resolution_required_cnt) {
  for (uint64_t slot_idx = 0; slot_idx < slots->size(); slot_idx++) {
    auto& slot = (*slots)[slot_idx];
    if (slot.primary() == nullptr) {
      ENVOY_LOG(debug,
                "starting async DNS resolution for primary slot address {} at index location {}",
                slot.primary_hostname_, slot_idx);
      parent_.dns_resolver_->resolve(
          slot.primary_hostname_, parent_.dns_lookup_family_,
          [this, slot_idx, slots, hostname_resolution_required_cnt](
              Network::DnsResolver::ResolutionStatus status, absl::string_view,
              std::list<Network::DnsResponse>&& response) -> void {
            auto& slot = (*slots)[slot_idx];
            ENVOY_LOG(
                debug,
                "async DNS resolution complete for primary slot address {} at index location {}",
                slot.primary_hostname_, slot_idx);
            updateDnsStats(status, response.empty());
            // If DNS resolution for a primary fails, we stop resolution for remaining, and reset
            // the timer.
            if (status != Network::DnsResolver::ResolutionStatus::Completed) {
              ENVOY_LOG(error, "Unable to resolve cluster slot primary hostname {}",
                        slot.primary_hostname_);
              resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
              return;
            }
            // A successful query can return an empty response.
            if (response.empty()) {
              ENVOY_LOG(error, "DNS resolution for primary slot address {} returned no results",
                        slot.primary_hostname_);
              resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
              return;
            }
            // Primary slot address resolved
            slot.setPrimary(Network::Utility::getAddressWithPort(
                *response.front().addrInfo().address_, slot.primary_port_));
            (*hostname_resolution_required_cnt)--;
            // Continue on to resolve replicas
            resolveReplicas(slots, slot_idx, hostname_resolution_required_cnt);
          });
    } else {
      resolveReplicas(slots, slot_idx, hostname_resolution_required_cnt);
    }
  }
}

/**
 * Resolve the replicas in a cluster entry. If there are no replicas, simply return.
 * If all the hostnames have been resolved, call post-resolution methods.
 * Failure to resolve a replica does not stop the overall resolution process. We log a
 * warning, and move to the next one.
 *
 * @param slots the list of slots which may need DNS resolution
 * @param index the specific index into `slots` whose replicas need to be resolved
 * @param address_resolution_required_cnt the number of address that need to be resolved
 */
void RedisCluster::RedisDiscoverySession::resolveReplicas(
    ClusterSlotsSharedPtr slots, std::size_t index,
    std::shared_ptr<std::uint64_t> hostname_resolution_required_cnt) {
  auto& slot = (*slots)[index];
  if (slot.replicas_to_resolve_.empty()) {
    if (*hostname_resolution_required_cnt == 0) {
      finishClusterHostnameResolution(slots);
    }
    return;
  }

  for (uint64_t replica_idx = 0; replica_idx < slot.replicas_to_resolve_.size(); replica_idx++) {
    auto replica = slot.replicas_to_resolve_[replica_idx];
    ENVOY_LOG(debug, "starting async DNS resolution for replica address {}", replica.first);
    parent_.dns_resolver_->resolve(
        replica.first, parent_.dns_lookup_family_,
        [this, index, slots, replica_idx, hostname_resolution_required_cnt](
            Network::DnsResolver::ResolutionStatus status, absl::string_view,
            std::list<Network::DnsResponse>&& response) -> void {
          auto& slot = (*slots)[index];
          auto& replica = slot.replicas_to_resolve_[replica_idx];
          ENVOY_LOG(debug, "async DNS resolution complete for replica address {}", replica.first);
          updateDnsStats(status, response.empty());
          // If DNS resolution fails here, we move on to resolve other replicas in the list.
          // We log a warn message.
          if (status != Network::DnsResolver::ResolutionStatus::Completed) {
            ENVOY_LOG(warn, "Unable to resolve cluster replica address {}", replica.first);
          } else if (response.empty()) {
            // A successful query can return an empty response.
            ENVOY_LOG(warn, "DNS resolution for cluster replica address {} returned no results",
                      replica.first);
          } else {
            // Replica resolved
            slot.addReplica(Network::Utility::getAddressWithPort(
                *response.front().addrInfo().address_, replica.second));
          }
          (*hostname_resolution_required_cnt)--;
          // finish resolution if all the addresses have been resolved.
          if (*hostname_resolution_required_cnt <= 0) {
            finishClusterHostnameResolution(slots);
          }
        });
  }
}

void RedisCluster::RedisDiscoverySession::finishClusterHostnameResolution(
    ClusterSlotsSharedPtr slots) {
  parent_.onClusterSlotUpdate(std::move(slots));
  resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
}

void RedisCluster::RedisDiscoverySession::onResponse(
    NetworkFilters::Common::Redis::RespValuePtr&& value) {
  ENVOY_LOG(debug, "redis cluster slot request for '{}' succeeded", parent_.info_->name());
  current_request_ = nullptr;

  const uint32_t SlotRangeStart = 0;
  const uint32_t SlotRangeEnd = 1;
  const uint32_t SlotPrimary = 2;
  const uint32_t SlotReplicaStart = 3;

  auto cluster_slots = std::make_shared<std::vector<ClusterSlot>>();
  auto hostname_resolution_required_cnt = std::make_shared<std::uint64_t>(0);

  if (Runtime::runtimeFeatureEnabled("envoy.reloadable_features.redis_use_cluster_nodes")) {
    // https://redis.io/commands/cluster-nodes
    // CLUSTER NODES represents array of cluster nodes, like this:
    //
    // 07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004,hostname4 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
    // 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,hostname2 master - 0 1426238316232 2 connected 5461-10922 
    // 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,hostname3 master - 0 1426238318243 3 connected 10923-16383
    // 6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005,hostname5 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected    
    // 824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006,hostname6 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
    // e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001,hostname1 myself,master - 0 0 1 connected 0-5460
    //
    // CLUSTER NODES returns a bulk string containing text lines
    if (value->type() != NetworkFilters::Common::Redis::RespType::BulkString) {
      onUnexpectedResponse(value);
      return;
    }

    const std::string& nodes_response = value->asString();

    auto parseNodeAddress = [](absl::string_view addr_str, std::string& host_out,
                               uint16_t& port_out) -> bool {
      // Split by comma to separate hostname if present: "IP:Port@BusPort,Hostname"
      auto comma_parts = StringUtil::splitToken(addr_str, ",");
      if (comma_parts.empty()) {
        return false;
      }
      absl::string_view addr_part = comma_parts[0];

      // Split by @ to remove cluster bus port
      auto at_parts = StringUtil::splitToken(addr_part, "@");
      if (at_parts.empty()) {
        return false;
      }
      addr_part = at_parts[0];

      // Find last colon to get IP/hostname and port (rfind for IPv6 support)
      size_t colon_pos = addr_part.rfind(':');
      if (colon_pos == absl::string_view::npos) {
        return false;
      }

      host_out = std::string(addr_part.substr(0, colon_pos));

      uint32_t port;
      if (!absl::SimpleAtoi(addr_part.substr(colon_pos + 1), &port) || port > 0xffff) {
        return false;
      }
      port_out = static_cast<uint16_t>(port);
      return true;
    };

    // Map node_id to indices in cluster_slots where this master's slots are located
    absl::flat_hash_map<absl::string_view, std::vector<size_t>> master_slot_indices;
    // Map master_id to pending replicas (for slaves seen before their master)
    absl::flat_hash_map<absl::string_view, std::vector<std::pair<std::string, uint16_t>>>
        pending_replicas;

    for (absl::string_view line : StringUtil::splitToken(nodes_response, "\r\n")) {
      if (line.empty()) {
        continue;
      }

      // 0: node_id, 1: address, 2: flags, 3: master_id, 4: ping_sent,
      // 5: pong_recv, 6: config_epoch, 7: link_state, 8+: slot ranges
      std::vector<absl::string_view> fields = StringUtil::splitToken(line, " \t");

      if (fields.size() < 8) {
        ENVOY_LOG(warn, "redis cluster nodes: malformed line with {} fields: {}", fields.size(),
                  line);
        continue;
      }

      absl::string_view node_id = fields[0];
      absl::string_view address = fields[1];
      absl::string_view flags = fields[2];
      absl::string_view master_id = fields[3];

      std::string host;
      uint16_t port;
      if (!parseNodeAddress(address, host, port)) {
        ENVOY_LOG(warn, "redis cluster nodes: invalid address: {}", address);
        continue;
      }

      bool healthy_node = true;
      size_t last_comma = flags.rfind(',');
      absl::string_view last_flag = flags;

      if (last_comma != absl::string_view::npos) {
        last_flag = flags.substr(last_comma + 1);
      }

      if (last_flag == "fail" || last_flag == "handshake" || last_flag == "noaddr") {
        ENVOY_LOG(warn, "redis cluster nodes: skipping master node {} with flags: {}", node_id,
                  flags);
        healthy_node = false;
      }

      if (absl::StrContains(flags, "master")) {        
        absl::string_view last_flag = flags;

        for (size_t i = 8; i < fields.size(); ++i) {
          absl::string_view slot_field = fields[i];

          int64_t start, end;
          size_t dash_pos = slot_field.find('-');

          if (dash_pos != absl::string_view::npos) {
            // Range format: "start-end"
            if (!absl::SimpleAtoi(slot_field.substr(0, dash_pos), &start) ||
                !absl::SimpleAtoi(slot_field.substr(dash_pos + 1), &end)) {
              ENVOY_LOG(warn, "redis cluster nodes: invalid slot range: {}", slot_field);
              continue;
            }
          } else {
            // Single slot
            if (!absl::SimpleAtoi(slot_field, &start)) {
              ENVOY_LOG(warn, "redis cluster nodes: invalid slot: {}", slot_field);
              continue;
            }
            end = start;
          }

          auto primary_address = Network::Utility::parseInternetAddressNoThrow(host, port, false);
          ClusterSlot slot(start, end, primary_address);

          // Setting health to unhealthy to prevent further calls into this primary.
          slot->primary()->coarseHealth(healthy_node ? Upstream::Host::Health::Healthy : Upstream::Host::Health::Unhealthy);

          if (slot.primary() == nullptr) {
            // Address is potentially a hostname, save it for async DNS resolution
            slot.primary_hostname_ = host;
            slot.primary_port_ = port;
            (*hostname_resolution_required_cnt)++;
          }

          // Track where this master's slots are in the vector
          master_slot_indices[node_id].push_back(cluster_slots->size());
          cluster_slots->push_back(std::move(slot));
        }

        // Add any pending replicas for this master that were seen before this master
        auto pending_it = pending_replicas.find(node_id);
        if (pending_it != pending_replicas.end()) {
          for (const auto& [replica_host, replica_port] : pending_it->second) {
            for (size_t slot_idx : master_slot_indices[node_id]) {
              auto& slot = (*cluster_slots)[slot_idx];

              auto replica_address =
                  Network::Utility::parseInternetAddressNoThrow(replica_host, replica_port, false);
              if (replica_address) {
                slot.addReplica(std::move(replica_address));
              } else {
                slot.addReplicaToResolve(replica_host, replica_port);
                (*hostname_resolution_required_cnt)++;
              }
            }
          }
          pending_replicas.erase(pending_it);
        }
      } else if (absl::StrContains(flags, "slave")) {
        auto master_it = master_slot_indices.find(master_id);
        if (master_it != master_slot_indices.end()) {
          // Master already seen, add replica immediately to all its slots
          for (size_t slot_idx : master_it->second) {
            auto& slot = (*cluster_slots)[slot_idx];

            auto replica_address = Network::Utility::parseInternetAddressNoThrow(host, port, false);
            if (replica_address) {
              slot.addReplica(std::move(replica_address));
            } else {
              slot.addReplicaToResolve(host, port);
              (*hostname_resolution_required_cnt)++;
            }
          }
        } else {
          // Master not yet seen, defer adding this replica
          pending_replicas[master_id].emplace_back(host, port);
        }
      }
    }

    // Log warning for any replicas whose masters were never seen
    if (!pending_replicas.empty()) {
      for (const auto& [master_id, replicas] : pending_replicas) {
        ENVOY_LOG(warn, "redis cluster nodes: {} replica(s) reference unknown master: {}",
                  replicas.size(), master_id);
      }
    }
  } else {
    // https://redis.io/commands/cluster-slots
    // CLUSTER SLOTS represents nested array of redis instances, like this:
    //
    // 1) 1) (integer) 0                                      <-- start slot range
    //    2) (integer) 5460                                   <-- end slot range
    //
    //    3) 1) "127.0.0.1"                                   <-- primary slot IP ADDR(HOSTNAME)
    //       2) (integer) 30001                               <-- primary slot PORT
    //       3) "09dbe9720cda62f7865eabc5fd8857c5d2678366"
    //
    //    4) 1) "127.0.0.2"                                   <-- replica slot IP ADDR(HOSTNAME)
    //       2) (integer) 30004                               <-- replica slot PORT
    //       3) "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf"
    //
    // CLUSTER SLOTS returns an array
    if (value->type() != NetworkFilters::Common::Redis::RespType::Array ||
        value->asArray().empty()) {
      onUnexpectedResponse(value);
      return;
    }

    // Loop through the cluster slot response and error checks for each field.
    for (const NetworkFilters::Common::Redis::RespValue& part : value->asArray()) {
      if (part.type() != NetworkFilters::Common::Redis::RespType::Array) {
        onUnexpectedResponse(value);
        return;
      }

      // Row 1-2: Slot ranges
      const std::vector<NetworkFilters::Common::Redis::RespValue>& slot_range = part.asArray();
      if (slot_range.size() < 3 ||
          slot_range[SlotRangeStart].type() !=
              NetworkFilters::Common::Redis::RespType::Integer || // Start slot range is an
                                                                  // integer.
          slot_range[SlotRangeEnd].type() !=
              NetworkFilters::Common::Redis::RespType::Integer) { // End slot range is an
                                                                  // integer.
        onUnexpectedResponse(value);
        return;
      }

      // Row 3: Primary slot address
      if (!validateCluster(slot_range[SlotPrimary])) {
        onUnexpectedResponse(value);
        return;
      }
      // Try to parse primary slot address as IP address
      // It may fail in case the address is a hostname. If this is the case - we'll come back later
      // and try to resolve hostnames asynchronously. For example, AWS ElastiCache returns hostname
      // instead of IP address.
      ClusterSlot slot(slot_range[SlotRangeStart].asInteger(), slot_range[SlotRangeEnd].asInteger(),
                       ipAddressFromClusterEntry(slot_range[SlotPrimary].asArray()));
      if (slot.primary() == nullptr) {
        // Primary address is potentially a hostname, save it for async DNS resolution.
        const auto& array = slot_range[SlotPrimary].asArray();
        slot.primary_hostname_ = array[0].asString();
        slot.primary_port_ = array[1].asInteger();
        (*hostname_resolution_required_cnt)++;
      }

      // Row 4-N: Replica(s) addresses
      for (auto replica = std::next(slot_range.begin(), SlotReplicaStart);
           replica != slot_range.end(); ++replica) {
        if (!validateCluster(*replica)) {
          onUnexpectedResponse(value);
          return;
        }
        auto replica_address = ipAddressFromClusterEntry(replica->asArray());
        if (replica_address) {
          slot.addReplica(std::move(replica_address));
        } else {
          // Replica address is potentially a hostname, save it for async DNS resolution.
          const auto& array = replica->asArray();
          slot.addReplicaToResolve(array[0].asString(), array[1].asInteger());
          (*hostname_resolution_required_cnt)++;
        }
      }
      cluster_slots->push_back(std::move(slot));
    }
  }

  if (*hostname_resolution_required_cnt > 0) {
    // DNS resolution is required, defer finalizing the slot update until resolution is complete.
    resolveClusterHostnames(std::move(cluster_slots), hostname_resolution_required_cnt);
  } else {
    // All slots addresses were represented by IP/Port pairs.
    parent_.onClusterSlotUpdate(std::move(cluster_slots));
    resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
  }
}

// Ensure that Slot Cluster response has valid format
bool RedisCluster::RedisDiscoverySession::validateCluster(
    const NetworkFilters::Common::Redis::RespValue& value) {
  // Verify data types
  if (value.type() != NetworkFilters::Common::Redis::RespType::Array) {
    return false;
  }
  const auto& array = value.asArray();
  if (array.size() < 2 || array[0].type() != NetworkFilters::Common::Redis::RespType::BulkString ||
      array[1].type() != NetworkFilters::Common::Redis::RespType::Integer) {
    return false;
  }
  // Verify IP/Host address
  if (array[0].asString().empty()) {
    return false;
  }
  // Verify port
  if (array[1].asInteger() > 0xffff) {
    return false;
  }

  return true;
}

void RedisCluster::RedisDiscoverySession::onUnexpectedResponse(
    const NetworkFilters::Common::Redis::RespValuePtr& value) {
  ENVOY_LOG(warn, "Unexpected response to cluster slot command: {}", value->toString());
  this->parent_.info_->configUpdateStats().update_failure_.inc();
  resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
}

void RedisCluster::RedisDiscoverySession::onFailure() {
  ENVOY_LOG(debug, "redis cluster slot request for '{}' failed", parent_.info_->name());
  current_request_ = nullptr;
  if (!current_host_address_.empty()) {
    auto client_to_delete = client_map_.find(current_host_address_);
    client_to_delete->second->client_->close();
  }
  parent_.info()->configUpdateStats().update_failure_.inc();
  resolve_timer_->enableTimer(parent_.cluster_refresh_rate_);
}

RedisCluster::ClusterSlotsRequest RedisCluster::ClusterSlotsRequest::instance_;
RedisCluster::ClusterNodesRequest RedisCluster::ClusterNodesRequest::instance_;

absl::StatusOr<std::pair<Upstream::ClusterImplBaseSharedPtr, Upstream::ThreadAwareLoadBalancerPtr>>
RedisClusterFactory::createClusterWithConfig(
    const envoy::config::cluster::v3::Cluster& cluster,
    const envoy::extensions::clusters::redis::v3::RedisClusterConfig& proto_config,
    Upstream::ClusterFactoryContext& context) {
  if (!cluster.has_cluster_type() || cluster.cluster_type().name() != "envoy.clusters.redis") {
    return absl::InvalidArgumentError("Redis cluster can only created with redis cluster type.");
  }
  auto resolver =
      THROW_OR_RETURN_VALUE(selectDnsResolver(cluster, context), Network::DnsResolverSharedPtr);
  // TODO(hyang): This is needed to migrate existing cluster, disallow using other lb_policy
  // in the future
  absl::Status creation_status = absl::OkStatus();
  if (cluster.lb_policy() != envoy::config::cluster::v3::Cluster::CLUSTER_PROVIDED) {
    auto ret =
        std::make_pair(std::shared_ptr<RedisCluster>(new RedisCluster(
                           cluster, proto_config, context,
                           NetworkFilters::Common::Redis::Client::ClientFactoryImpl::instance_,
                           resolver, nullptr, creation_status)),
                       nullptr);
    RETURN_IF_NOT_OK(creation_status);
    return ret;
  }
  auto lb_factory = std::make_shared<RedisClusterLoadBalancerFactory>(
      context.serverFactoryContext().api().randomGenerator());
  absl::StatusOr<std::unique_ptr<RedisCluster>> cluster_or_error = RedisCluster::create(
      cluster, proto_config, context,
      NetworkFilters::Common::Redis::Client::ClientFactoryImpl::instance_, resolver, lb_factory);
  RETURN_IF_NOT_OK(cluster_or_error.status());
  return std::make_pair(std::shared_ptr<RedisCluster>(std::move(*cluster_or_error)),
                        std::make_unique<RedisClusterThreadAwareLoadBalancer>(lb_factory));
}

REGISTER_FACTORY(RedisClusterFactory, Upstream::ClusterFactory);

} // namespace Redis
} // namespace Clusters
} // namespace Extensions
} // namespace Envoy
