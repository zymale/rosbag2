// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Python.h>
#include <chrono>
#include <string>
#include <vector>

#include "rosbag2_transport/rosbag2_transport.hpp"
#include "rosbag2_transport/record_options.hpp"
#include "rosbag2_transport/storage_options.hpp"
#include "rmw/rmw.h"

static void
_rosbag_destruct_transport(PyObject * obj)
{
  auto transport = static_cast<rosbag2_transport::Rosbag2Transport *>(
    PyCapsule_GetPointer(obj, "rosbag2_transport::Rosbag2Transport"));
  if (nullptr == transport) {
    PyErr_Clear();
    // log stderr
  }
  delete transport;
}

static PyObject *
rosbag2_transport_create(PyObject * Py_UNUSED(self), PyObject * args)
{
  rosbag2_transport::NodeOptions node_options{};
  rosbag2_transport::StorageOptions storage_options{};

  char * node_prefix = nullptr;
  char * uri = nullptr;
  char * storage_id = nullptr;
  if (!PyArg_ParseTuple(args, "sss", &node_prefix, &uri, &storage_id)) {
    return nullptr;
  }

  node_options.node_prefix = node_prefix;
  node_options.standalone = true;

  storage_options.uri = std::string(uri);
  storage_options.storage_id = std::string(storage_id);

  auto transport = new rosbag2_transport::Rosbag2Transport(node_options, storage_options);
  PyObject * pytransport = PyCapsule_New(
    transport, "rosbag2_transport::Rosbag2Transport", _rosbag_destruct_transport);
  if (nullptr == pytransport) {
    delete transport;
    transport = nullptr;
    PyMem_Free(transport);
    return nullptr;
  }

  return pytransport;
}

static PyObject *
rosbag2_transport_record(PyObject * Py_UNUSED(self), PyObject * args, PyObject * kwargs)
{
  rosbag2_transport::RecordOptions record_options{};

  static const char * kwlist[] = {
    "",
    "serialization_format",
    "all",
    "no_discovery",
    "polling_interval",
    "topics",
    nullptr};

  PyObject * pytransport = nullptr;
  char * serilization_format = nullptr;
  bool all = false;
  bool no_discovery = false;
  uint64_t polling_interval_ms = 100;
  PyObject * topics = nullptr;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os|bbKO", const_cast<char **>(kwlist),
    &pytransport,
    &serilization_format,
    &all,
    &no_discovery,
    &polling_interval_ms,
    &topics))
  {
    return nullptr;
  }

  auto transport = static_cast<rosbag2_transport::Rosbag2Transport *>(
    PyCapsule_GetPointer(pytransport, "rosbag2_transport::Rosbag2Transport"));
  if (nullptr == transport) {
    return nullptr;
  }

  record_options.all = all;
  record_options.is_discovery_disabled = no_discovery;
  record_options.topic_polling_interval = std::chrono::milliseconds(polling_interval_ms);

  if (topics) {
    PyObject * topic_iterator = PyObject_GetIter(topics);
    if (topic_iterator != nullptr) {
      PyObject * topic;
      while ((topic = PyIter_Next(topic_iterator))) {
        record_options.topics.emplace_back(PyUnicode_AsUTF8(topic));

        Py_DECREF(topic);
      }
      Py_DECREF(topic_iterator);
    }
  }
  record_options.rmw_serialization_format = std::string(serilization_format).empty() ?
    rmw_get_serialization_format() :
    serilization_format;

  transport->record(record_options);

  Py_RETURN_NONE;
}

static PyObject *
rosbag2_transport_play(PyObject * Py_UNUSED(self), PyObject * args, PyObject * kwargs)
{
  rosbag2_transport::PlayOptions play_options{};

  static const char * kwlist[] = {
    "",
    "read_ahead_queue_size",
    "paused",
    nullptr
  };

  PyObject * pytransport = nullptr;
  size_t read_ahead_queue_size = 1000;
  bool paused = false;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Okb", const_cast<char **>(kwlist),
    &pytransport,
    &read_ahead_queue_size,
    &paused))
  {
    return nullptr;
  }

  auto transport = static_cast<rosbag2_transport::Rosbag2Transport *>(
    PyCapsule_GetPointer(pytransport, "rosbag2_transport::Rosbag2Transport"));
  if (nullptr == transport) {
    return nullptr;
  }

  play_options.read_ahead_queue_size = read_ahead_queue_size;
  play_options.paused = paused;

  transport->play(play_options);

  Py_RETURN_NONE;
}

static PyObject *
rosbag2_transport_info(PyObject * Py_UNUSED(self), PyObject * args)
{
  PyObject * pytransport = nullptr;
  if (!PyArg_ParseTuple(
      args, "O", &pytransport))
  {
    return nullptr;
  }

  auto transport = static_cast<rosbag2_transport::Rosbag2Transport *>(
    PyCapsule_GetPointer(pytransport, "rosbag2_transport::Rosbag2Transport"));
  if (nullptr == transport) {
    return nullptr;
  }

  transport->print_bag_info();

  Py_RETURN_NONE;
}

/// Define the public methods of this module
static PyMethodDef rosbag2_transport_methods[] = {
  {
    "create", rosbag2_transport_create, METH_VARARGS, "Get transport instance"
  },
  {
    "record", reinterpret_cast<PyCFunction>(rosbag2_transport_record),
    METH_VARARGS | METH_KEYWORDS, "Record to bag"
  },
  {
    "play", reinterpret_cast<PyCFunction>(rosbag2_transport_play),
    METH_VARARGS | METH_KEYWORDS, "Play bag"
  },
  {
    "info", rosbag2_transport_info, METH_VARARGS, "Print bag info"
  },
  {nullptr, nullptr, 0, nullptr}  /* sentinel */
};

PyDoc_STRVAR(rosbag2_transport__doc__,
  "Python module for rosbag2 transport");

/// Define the Python module
static struct PyModuleDef _rosbag2_transport_module = {
  PyModuleDef_HEAD_INIT,
  "_rosbag2_transport",
  rosbag2_transport__doc__,
  -1,   /* -1 means that the module keeps state in global variables */
  rosbag2_transport_methods,
  nullptr,
  nullptr,
  nullptr,
  nullptr
};

/// Init function of this module
PyMODINIT_FUNC PyInit__rosbag2_transport_py(void)
{
  return PyModule_Create(&_rosbag2_transport_module);
}
