import 'package:dart_nats/dart_nats.dart';
import 'package:test/test.dart';

var port = 8084;

void main() {
  group('all', () {
    test('seed', () async {
      var nkeys = await Nkeys.fromSeed(
          'SUAKJESHKJ5POJJINJFMCVYAASA7LQTL5ZOMMTYOWRZCM3JRZRS3OIVKZA');
      var s = nkeys.seed;
      expect(s,
          equals('SUAKJESHKJ5POJJINJFMCVYAASA7LQTL5ZOMMTYOWRZCM3JRZRS3OIVKZA'));
    });
    // test('private key', () async {
    //   var nkeys = await Nkeys.fromSeed(
    //       'SUAKJESHKJ5POJJINJFMCVYAASA7LQTL5ZOMMTYOWRZCM3JRZRS3OIVKZA');
    //   var p = await nkeys.privateKey();

    //   expect(p,
    //       equals('UBYKMUQEJ7U2KFHB37IUOX6NBTJAWGY6SDO3DFRVOBNXVDUPPOTNWXD5'));
    // });
  });
}
