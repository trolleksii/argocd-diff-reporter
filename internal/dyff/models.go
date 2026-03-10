// Copyright (c) 2019 The Homeport Team
//
// This file contains code derived from https://github.com/homeport/dyff,
// licensed under the MIT License. See THIRD_PARTY_NOTICES for details.

package dyff

import (
	"github.com/gonvenience/ytbx"
	yamlv3 "go.yaml.in/yaml/v3"
)

const (
	ADDITION     = '+'
	REMOVAL      = '-'
	MODIFICATION = '±'
	ORDERCHANGE  = '⇆'
)

type Detail struct {
	From *yamlv3.Node
	To   *yamlv3.Node
	Kind rune
}

type Diff struct {
	Path    *ytbx.Path
	Details []Detail
}
