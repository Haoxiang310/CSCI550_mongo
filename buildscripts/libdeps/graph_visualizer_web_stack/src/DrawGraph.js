import React, { useRef, useEffect } from "react";
import * as THREE from "three";
import { connect } from "react-redux";
import ForceGraph2D from "react-force-graph-2d";
import ForceGraph3D from "react-force-graph-3d";
import SwitchComponents from "./SwitchComponent";
import Button from "@material-ui/core/Button";
import TextField from "@material-ui/core/TextField";

import theme from "./theme";
import { socket } from "./connect";
import { getGraphData } from "./redux/store";
import { updateCheckbox } from "./redux/nodes";
import { setFindNode } from "./redux/findNode";
import LoadingBar from "./LoadingBar";

const handleFindNode = (node_value, graphData, activeComponent, forceRef) => {
  var targetNode = null;
  if (graphData) {
    for (var i = 0; i < graphData.nodes.length; i++) {
      var node = graphData.nodes[i];
      if (node.name == node_value || node.id == node_value) {
        targetNode = node;
        break;
      }
    }
    if (targetNode != null) {
      if (activeComponent == "3D") {
        if (forceRef.current != null) {
          forceRef.current.centerAt(targetNode.x, targetNode.y, 2000);
          forceRef.current.zoom(6, 1000);
        }
      } else {
        const distance = 100;
        const distRatio =
          1 + distance / Math.hypot(targetNode.x, targetNode.y, targetNode.z);
        if (forceRef.current != null) {
          forceRef.current.cameraPosition(
            {
              x: targetNode.x * distRatio,
              y: targetNode.y * distRatio,
              z: targetNode.z * distRatio,
            }, // new position
            targetNode, // lookAt ({ x, y, z })
            3000 // ms transition duration
          );
        }
      }
    }
  }
};

const DrawGraph = ({
  size,
  graphData,
  nodes,
  loading,
  graphPaths,
  updateCheckbox,
  findNode,
  setFindNode,
}) => {
  const [activeComponent, setActiveComponent] = React.useState("2D");
  const [selectedNodes, setSelectedNodes] = React.useState([]);
  const [pathNodes, setPathNodes] = React.useState({});
  const [pathEdges, setPathEdges] = React.useState([]);
  const forceRef = useRef(null);

  React.useEffect(() => {
    handleFindNode(findNode, graphData, activeComponent, forceRef);
    setFindNode("");
  }, [findNode, graphData, activeComponent, forceRef]);

  React.useEffect(() => {
    setSelectedNodes(
      nodes.map((node) => {
        if (node.selected) {
          return node.node;
        }
      })
    );
  }, [nodes]);

  React.useEffect(() => {
    setPathNodes({ fromNode: graphPaths.fromNode, toNode: graphPaths.toNode });
    var paths = Array();
    for (var path = 0; path < graphPaths.paths.length; path++) {
      var pathArr = Array();
      for (var i = 0; i < graphPaths.paths[path].length; i++) {
        if (i == 0) {
          continue;
        }
        pathArr.push({
          source: graphPaths.paths[path][i - 1],
          target: graphPaths.paths[path][i],
        });
      }
      paths.push(pathArr);
    }
    setPathEdges(paths);
  }, [graphPaths]);

  React.useEffect(() => {
    if (forceRef.current != null) {
      if (activeComponent == '3D'){
        forceRef.current.d3Force("charge").strength(-2000);
      }
      else {
        forceRef.current.d3Force("charge").strength(-10000);
      }

    }
  }, [forceRef.current, activeComponent]);

  const paintRing = React.useCallback(
    (node, ctx) => {
      // add ring just for highlighted nodes
      ctx.beginPath();
      ctx.arc(node.x, node.y, 7 * 1.4, 0, 2 * Math.PI, false);
      if (node.id == pathNodes.fromNode) {
        ctx.fillStyle = "blue";
      } else if (node.id == pathNodes.toNode) {
        ctx.fillStyle = "red";
      } else {
        ctx.fillStyle = "green";
      }
      ctx.fill();
    },
    [pathNodes]
  );

  function colorNodes(node) {
    switch (node.type) {
      case "SharedLibrary":
        return "#e6ed11"; // yellow
      case "Program":
        return "#1120ed"; // blue
      case "shim":
        return "#800303"; // dark red
      default:
        return "#5a706f"; // grey
    }
  }

  return (
    <LoadingBar loading={loading} height={"100%"}>
      <Button
        onClick={() => {
          if (activeComponent == "2D") {
            setActiveComponent("3D");
          } else {
            setActiveComponent("2D");
          }
        }}
      >
        {activeComponent}
      </Button>
      <TextField
        size="small"
        label="Find Node"
        onChange={(event) => {
          handleFindNode(
            event.target.value,
            graphData,
            activeComponent,
            forceRef
          );
        }}
      />
      <SwitchComponents active={activeComponent}>
        <ForceGraph2D
          name="3D"
          width={size}
          dagMode="radialout"
          dagLevelDistance={50}
          graphData={graphData}
          ref={forceRef}
          nodeColor={colorNodes}
          nodeOpacity={1}
          backgroundColor={theme.palette.secondary.dark}
          linkDirectionalArrowLength={6}
          linkDirectionalArrowRelPos={1}
          linkDirectionalParticles={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return 5;
                }
              }
            }
            return 0;
          }}
          linkDirectionalParticleSpeed={(d) => {
            return 0.01;
          }}
          nodeCanvasObjectMode={(node) => {
            if (selectedNodes.includes(node.id)) {
              return "before";
            }
          }}
          linkColor={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return "#12FF19";
                }
              }
            }
            return "#FAFAFA";
          }}
          linkDirectionalParticleWidth={6}
          linkWidth={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return 2;
                }
              }
            }
            return 1;
          }}
          nodeRelSize={7}
          nodeCanvasObject={paintRing}
          onNodeClick={(node, event) => {
            updateCheckbox({ node: node.id, value: "flip" });
            socket.emit("row_selected", {
              data: { node: node.id, name: node.name },
              isSelected: !selectedNodes.includes(node.id),
            });
          }}
        />
        <ForceGraph3D
          name="2D"
          width={size}
          dagMode="radialout"
          graphData={graphData}
          nodeColor={colorNodes}
          nodeOpacity={1}
          nodeThreeObject={(node) => {
            if (!selectedNodes.includes(node.id)) {
              return new THREE.Mesh(
                new THREE.SphereGeometry(5, 5, 5),
                new THREE.MeshLambertMaterial({
                  color: colorNodes(node),
                  transparent: true,
                  opacity: 0.2,
                })
              );
            }
          }}
          onNodeClick={(node, event) => {
            updateCheckbox({ node: node.id, value: "flip" });
            socket.emit("row_selected", {
              data: { node: node.id, name: node.name },
              isSelected: !selectedNodes.includes(node.id),
            });
          }}
          linkColor={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return "#12FF19";
                }
              }
            }
            return "#FAFAFA";
          }}
          linkDirectionalParticleWidth={7}
          linkWidth={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return 5;
                }
              }
            }
            return 1;
          }}
          linkDirectionalParticles={(d) => {
            if (graphPaths.selectedPath >= 0) {
              for (
                var i = 0;
                i < pathEdges[graphPaths.selectedPath].length;
                i++
              ) {
                if (
                  pathEdges[graphPaths.selectedPath][i].source == d.source.id &&
                  pathEdges[graphPaths.selectedPath][i].target == d.target.id
                ) {
                  return 5;
                }
              }
            }
            return 0;
          }}
          linkDirectionalParticleSpeed={(d) => {
            return 0.01;
          }}
          linkDirectionalParticleResolution={10}
          linkOpacity={0.6}
          nodeRelSize={7}
          backgroundColor={theme.palette.secondary.dark}
          linkDirectionalArrowLength={3.5}
          linkDirectionalArrowRelPos={1}
          ref={forceRef}
        />
      </SwitchComponents>
    </LoadingBar>
  );
};

export default connect(getGraphData, { setFindNode, updateCheckbox })(
  DrawGraph
);
