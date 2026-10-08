#!/bin/sh
# Keep a connection to the IOSP datanetwork meeting points.
# Peer IDs are the kubo_id values in iosp-datanetwork-test/ops/meeting-points.json.
# Empty addresses are resolved through the public DHT.
set -eu
ipfs config --json Peering.Peers '[{"ID":"12D3KooWJsbtcPjYbsEd2pDG1JPWAx37qoW3S4rbgyNsVkArDxFX","Addrs":[]},{"ID":"12D3KooWJZJS2rsP7KRBhiuQUCeqwwcJpWkJmJ2d7LafsxQooVaJ","Addrs":[]}]'
