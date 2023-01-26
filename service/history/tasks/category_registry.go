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
	"sync"

	"golang.org/x/exp/maps"
)

// CategoryRegistry is a registry that contains all categories.
type CategoryRegistry interface {
	// RegisterCategory registers a Category
	// It returns a wrapped ErrCategoryAlreadyRegistered error if the Category is already registered
	RegisterCategory(category Category) error
	// GetCategories returns a deep copy of all registered Categories
	GetCategories() map[int32]Category
	// GetCategoryByID returns a registered Category with the same ID
	// It returns a bool indicating whether the Category is found
	GetCategoryByID(id int32) (Category, bool)
}

// NewDefaultCategoryRegistry returns a CategoryRegistry with all default Categories registered.
func NewDefaultCategoryRegistry() CategoryRegistry {
	return &categoryRegistry{categories: map[int32]Category{
		CategoryTransfer.ID():    CategoryTransfer,
		CategoryTimer.ID():       CategoryTimer,
		CategoryVisibility.ID():  CategoryVisibility,
		CategoryReplication.ID(): CategoryReplication,
	}}
}

func newCategoryRegistry() CategoryRegistry {
	return &categoryRegistry{
		categories: make(map[int32]Category),
	}
}

type categoryRegistry struct {
	lock       sync.RWMutex
	categories map[int32]Category
}

var (
	ErrorCategoryAlreadyRegistered = errors.New("category already registered")
)

func (r *categoryRegistry) RegisterCategory(category Category) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	id := category.ID()
	if old, ok := r.categories[id]; ok {
		return fmt.Errorf(
			"%w: can't register %+v as because %+v is already registered for id %d",
			ErrorCategoryAlreadyRegistered,
			category,
			old,
			id,
		)
	}
	r.categories[id] = category
	return nil
}

func (r *categoryRegistry) GetCategories() map[int32]Category {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return maps.Clone(r.categories)
}

func (r *categoryRegistry) GetCategoryByID(id int32) (Category, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	category, ok := r.categories[id]
	return category, ok
}
