// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tasks

import (
	"strconv"

	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	Category struct {
		id    int32
		cType CategoryType
		name  string
	}

	CategoryType int
)

func NewCategory(id int32, cType CategoryType, name string) Category {
	return Category{
		id:    id,
		cType: cType,
		name:  name,
	}
}

const (
	CategoryIDUnspecified = int32(enumsspb.TASK_CATEGORY_UNSPECIFIED)
	CategoryIDTransfer    = int32(enumsspb.TASK_CATEGORY_TRANSFER)
	CategoryIDTimer       = int32(enumsspb.TASK_CATEGORY_TIMER)
	CategoryIDReplication = int32(enumsspb.TASK_CATEGORY_REPLICATION)
	CategoryIDVisibility  = int32(enumsspb.TASK_CATEGORY_VISIBILITY)
	CategoryIDArchival    = int32(enumsspb.TASK_CATEGORY_ARCHIVAL)
)

const (
	CategoryTypeUnspecified CategoryType = iota
	CategoryTypeImmediate
	CategoryTypeScheduled
)

const (
	CategoryNameTransfer    = "transfer"
	CategoryNameTimer       = "timer"
	CategoryNameReplication = "replication"
	CategoryNameVisibility  = "visibility"
	CategoryNameArchival    = "archival"
)

var (
	CategoryTransfer = Category{
		id:    CategoryIDTransfer,
		cType: CategoryTypeImmediate,
		name:  CategoryNameTransfer,
	}

	CategoryTimer = Category{
		id:    CategoryIDTimer,
		cType: CategoryTypeScheduled,
		name:  CategoryNameTimer,
	}

	CategoryReplication = Category{
		id:    CategoryIDReplication,
		cType: CategoryTypeImmediate,
		name:  CategoryNameReplication,
	}

	CategoryVisibility = Category{
		id:    CategoryIDVisibility,
		cType: CategoryTypeImmediate,
		name:  CategoryNameVisibility,
	}

	CategoryArchival = Category{
		id:    CategoryIDArchival,
		cType: CategoryTypeScheduled,
		name:  CategoryNameArchival,
	}
)

func (c *Category) ID() int32 {
	return c.id
}

func (c *Category) Name() string {
	return c.name
}

func (c *Category) Type() CategoryType {
	return c.cType
}

func (t CategoryType) String() string {
	switch t {
	case CategoryTypeImmediate:
		return "Immediate"
	case CategoryTypeScheduled:
		return "Scheduled"
	default:
		return strconv.Itoa(int(t))
	}
}
