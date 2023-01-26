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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestCategoryRegistry_RegisterCategory(t *testing.T) {
	t.Parallel()

	registry := newCategoryRegistry()
	err := registry.RegisterCategory(Category{
		id:   100,
		name: "foo",
	})
	assert.NoError(t, err)

	err = registry.RegisterCategory(Category{
		id:   100,
		name: "bar",
	})
	assert.ErrorIs(t, err, ErrorCategoryAlreadyRegistered)
	assert.ErrorContains(t, err, "foo")
	assert.ErrorContains(t, err, "bar")
	assert.ErrorContains(t, err, "100")
}

func TestCategoryRegistry_GetCategories(t *testing.T) {
	t.Parallel()

	registry := newCategoryRegistry()
	require.NoError(t, registry.RegisterCategory(Category{
		id:    100,
		cType: CategoryTypeImmediate,
		name:  "test1",
	}))
	require.NoError(t, registry.RegisterCategory(Category{
		id:    200,
		cType: CategoryTypeScheduled,
		name:  "test2",
	}))

	assert.Equal(t, map[int32]Category{
		100: {
			id:    100,
			cType: CategoryTypeImmediate,
			name:  "test1",
		},
		200: {
			id:    200,
			cType: CategoryTypeScheduled,
			name:  "test2",
		},
	}, registry.GetCategories())
}

func TestCategoryRegistry_GetCategoryByID(t *testing.T) {
	t.Parallel()

	var registry CategoryRegistry
	fxtest.New(t, Module, fx.Populate(&registry))
	registry.GetCategories()

	require.NoError(t, registry.RegisterCategory(Category{
		id:    100,
		cType: CategoryTypeImmediate,
		name:  "test1",
	}))

	category, ok := registry.GetCategoryByID(100)
	assert.True(t, ok)
	assert.Equal(t, Category{
		id:    100,
		cType: CategoryTypeImmediate,
		name:  "test1",
	}, category)
}
