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
	"errors"
	"fmt"

	"golang.org/x/exp/maps"
)

// CategoryRegistry is a mutable registry of all registered Categories. If you want to add a new Category, and you are
// using fx, you should write a decorator function such as:
//
//	func addMyCategory(registry tasks.CategoryRegistry) (tasks.CategoryRegistry, error) {
//		err := registry.RegisterCategory(myCategory)
//		return registry, err
//	}
//
// and then add it to the fx graph using fx.Decorate(addMyCategory)
type CategoryRegistry interface {
	// RegisterCategory registers a Category
	// It returns a wrapped ErrCategoryAlreadyRegistered error if the Category is already registered.
	// This method is not thread-safe, but it doesn't need to be if you are using fx.
	RegisterCategory(category Category) error
	// BuildCategoryIndex builds a CategoryIndex.
	// If you call RegisterCategory after this method is called, existing CategoryIndex objects will not be updated,
	// but subsequent calls to BuildCategoryIndex will return a CategoryIndex with the new Category.
	BuildCategoryIndex() CategoryIndex
}

var (
	// ErrCategoryAlreadyRegistered is returned when a Category is already registered
	ErrCategoryAlreadyRegistered = errors.New("category already registered")
)

// newCategoryRegistry creates a new CategoryRegistry with no registered Categories
func newCategoryRegistry() CategoryRegistry {
	return &categoryRegistry{
		categories: make(map[int32]Category),
	}
}

type categoryRegistry struct {
	categories map[int32]Category
}

func (r *categoryRegistry) RegisterCategory(category Category) error {
	id := category.ID()
	if old, ok := r.categories[id]; ok {
		return fmt.Errorf(
			"%w: can't register %+v as because %+v is already registered for id %d",
			ErrCategoryAlreadyRegistered,
			category,
			old,
			id,
		)
	}
	r.categories[id] = category
	return nil
}

func (r *categoryRegistry) BuildCategoryIndex() CategoryIndex {
	return &categoryIndex{
		categories: maps.Clone(r.categories),
	}
}
