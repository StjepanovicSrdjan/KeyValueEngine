package MerkleTree

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"io/ioutil"
	"os"
	"strings"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func CreateLeaves(data [][]byte) []*Node {
	leaves := make([]*Node, 0, 1)
	for i := range data {
		node := Node{data: Hash(data[i]), left: nil, right: nil}
		leaves = append(leaves, &node)
	}
	return leaves
}

func InitMerkleTree(data [][]byte) *MerkleRoot {
	leaves := CreateLeaves(data)
	merkleTree := MerkleRoot{root: nil}
	merkleTree.root = merkleTree.BottomUpBuild(leaves)
	return &merkleTree
}

func (merkleTree *MerkleRoot) BottomUpBuild(leaves []*Node) *Node {
	parents := make([]*Node, 0, 1)

	if len(leaves)%2 != 0 {
		emptyNode := Node{data: Hash(make([]byte, 0, 0)), left: nil, right: nil}
		leaves = append(leaves, &emptyNode)
	}

	for i := 0; i < len(leaves); i += 2 {
		leftChild := leaves[i]
		rightChild := leaves[i+1]
		combinedData := make([]byte, 0, 1)
		combinedData = append(combinedData, leftChild.data[:]...)
		combinedData = append(combinedData, rightChild.data[:]...)
		parents = append(parents, &Node{data: Hash(combinedData), left: leftChild, right: rightChild})
	}

	if len(parents) == 1 {
		merkleTree.root = parents[0]
		return parents[0]
	}else if len(parents) > 1 {
		return merkleTree.BottomUpBuild(parents)
	}else {
		panic("MerkleTree build error.")
	}
}

func (merkleTree *MerkleRoot) Serialize(path string) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	nodeList := make([]*Node, 0, 1)
	nodeList = append(nodeList, merkleTree.root)
	queue := make([]*Node, 0, 1)
	queue = append(queue, merkleTree.root)

	for len(nodeList) > 0{
		currentNode := queue[0]
		queue = queue[1:]

		// add is nil check if needed
		queue = append(queue, currentNode.left)
		queue = append(queue, currentNode.right)
		nodeList = append(nodeList, currentNode.left)
		nodeList = append(nodeList, currentNode.right)
	}

	for i := range(nodeList) {
		_, err = writer.Write([]byte(nodeList[i].String() + ";"))
		if err != nil {
			panic(err)
		}
	}
	writer.Flush()
}

func (merkleTree *MerkleRoot) Deserialize(path string) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	nodeHashes := strings.Split(string(data), ";")
	nodeHashes = nodeHashes[:len(nodeHashes)-1]

	file.Close()

	nodes := make([]*Node, 0, 1)
	for i := 0; i < len(nodeHashes); i++ {
		decodedHash, _ := hex.DecodeString(nodeHashes[i])
		var data [20]byte
		copy(data[:], decodedHash)
		nodes[i] = &Node{data: data, left: nil, right: nil}
	}

	if len(nodes) == 0 {
		merkleTree.root = nil
		return
	}

	// we can be sure that merkle tree is complete, so when index is out of range we are finished setting node children
	for index := range(nodes) {
		indexLeft := 2 * index + 1
		indexRight := 2 * index + 2
		if indexRight >= len(nodes) || indexLeft >= len(nodes){
			break
		}
		nodes[index].left = nodes[indexLeft]
		nodes[index].right = nodes[indexRight]
	}

	merkleTree.root = nodes[0]
}
