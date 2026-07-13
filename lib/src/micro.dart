import 'dart:async';
import 'dart:convert';
import 'client.dart';
import 'message.dart';
import 'subscription.dart';
import 'inbox.dart';

/// Configuration for NATS Microservice
class ServiceConfig {
  /// The service name (alphanumeric, dots, dashes, underscores)
  final String name;

  /// The service version (semver format)
  final String version;

  /// Optional service description
  final String? description;

  /// Optional service metadata
  final Map<String, String>? metadata;

  /// List of endpoints managed by the service
  final List<Endpoint> endpoints;

  /// Constructor for ServiceConfig
  ServiceConfig({
    required this.name,
    required this.version,
    this.description,
    this.metadata,
    this.endpoints = const [],
  });
}

/// Endpoint definition within a Microservice
class Endpoint {
  /// Name of the endpoint
  final String name;

  /// NATS subject the endpoint listens on
  final String subject;

  /// Request handler function
  final FutureOr<void> Function(Message msg) handler;

  /// Optional endpoint metadata
  final Map<String, String>? metadata;

  /// Constructor for Endpoint
  Endpoint({
    required this.name,
    required this.subject,
    required this.handler,
    this.metadata,
  });
}

/// Statistics tracker for an Endpoint
class EndpointStats {
  /// Name of the endpoint
  final String name;

  /// Subject of the endpoint
  final String subject;

  /// Total number of requests processed
  int numRequests = 0;

  /// Total number of errors encountered
  int numErrors = 0;

  /// Description of the last error
  String lastError = '';

  /// Total processing time elapsed
  Duration totalProcessingTime = Duration.zero;

  /// Constructor for EndpointStats
  EndpointStats({required this.name, required this.subject});

  /// Export stats to JSON map
  Map<String, dynamic> toJson() {
    final avgNs = numRequests == 0
        ? 0
        : (totalProcessingTime.inMicroseconds * 1000) ~/ numRequests;
    return {
      'name': name,
      'subject': subject,
      'num_requests': numRequests,
      'num_errors': numErrors,
      'last_error': lastError,
      'processing_time': totalProcessingTime.inMicroseconds * 1000,
      'average_processing_time': avgNs,
    };
  }
}

/// Manages the NATS Microservice lifecycle (ADR-32)
class MicroService {
  /// The underlying NATS client
  final Client client;

  /// The service configuration
  final ServiceConfig config;

  /// Unique service instance ID
  final String id;

  /// Timestamp when the service started
  final DateTime started;

  final List<Subscription> _subscriptions = [];
  final Map<String, EndpointStats> _stats = {};

  /// Constructor for MicroService
  MicroService(this.client, this.config, this.id)
      : started = DateTime.now().toUtc() {
    for (var ep in config.endpoints) {
      _stats[ep.name] = EndpointStats(name: ep.name, subject: ep.subject);
    }
  }

  /// Start the microservice and subscribe to endpoints and system subjects
  Future<void> start() async {
    // 1. Subscribe to each service endpoint
    for (var ep in config.endpoints) {
      final epStats = _stats[ep.name]!;
      final sub = client.sub<dynamic>(ep.subject);
      sub.stream.listen((msg) async {
        final stopwatch = Stopwatch()..start();
        epStats.numRequests++;
        try {
          await ep.handler(msg);
        } catch (e) {
          epStats.numErrors++;
          epStats.lastError = e.toString();
        } finally {
          stopwatch.stop();
          epStats.totalProcessingTime += stopwatch.elapsed;
        }
      });
      _subscriptions.add(sub);
    }

    // 2. Subscribe to control/monitoring subjects
    final monitoringSubjects = [
      // PING
      '\$SRV.PING',
      '\$SRV.PING.${config.name}',
      '\$SRV.PING.${config.name}.$id',
      // INFO
      '\$SRV.INFO',
      '\$SRV.INFO.${config.name}',
      '\$SRV.INFO.${config.name}.$id',
      // STATS
      '\$SRV.STATS',
      '\$SRV.STATS.${config.name}',
      '\$SRV.STATS.${config.name}.$id',
    ];

    for (var subName in monitoringSubjects) {
      final sub = client.sub<dynamic>(subName);
      sub.stream.listen((msg) {
        if (msg.replyTo == null || msg.replyTo!.isEmpty) return;

        if (subName.contains('PING')) {
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.ping_response',
            'metadata': config.metadata ?? {},
          }));
        } else if (subName.contains('INFO')) {
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.info_response',
            'description': config.description ?? '',
            'metadata': config.metadata ?? {},
            'endpoints': config.endpoints
                .map((e) => {
                      'name': e.name,
                      'subject': e.subject,
                      'metadata': e.metadata ?? {},
                    })
                .toList(),
          }));
        } else if (subName.contains('STATS')) {
          msg.respondString(jsonEncode({
            'id': id,
            'name': config.name,
            'version': config.version,
            'type': 'io.nats.micro.v1.stats_response',
            'started': started.toIso8601String(),
            'stats': {
              'endpoints': _stats.values.map((s) => s.toJson()).toList(),
            }
          }));
        }
      });
      _subscriptions.add(sub);
    }
  }

  /// Stop the service and unsubscribe all subscriptions
  Future<void> stop() async {
    for (var sub in _subscriptions) {
      await sub.close();
    }
    _subscriptions.clear();
  }
}

/// Extension on Client class to add Microservice support
extension ClientMicroServiceExtension on Client {
  /// Register and start a NATS Microservice
  Future<MicroService> addService(ServiceConfig config) async {
    final id = Nuid().next();
    final service = MicroService(this, config, id);
    await service.start();
    return service;
  }
}
