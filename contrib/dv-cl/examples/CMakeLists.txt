build_lib_example(
  NAME dv-cl-toa-example
  SOURCE_FILES dv-cl-toa-example.cc
  LIBRARIES_TO_LINK ${libdv-cl}
)

build_lib_example(
  NAME dv-cl-mesh-example
  SOURCE_FILES dv-cl-mesh-example.cc
  LIBRARIES_TO_LINK ${libdv-cl}
)

build_lib_example(
  NAME dv-cl-campaign-example
  SOURCE_FILES
    campaign/dv-cl-campaign-example.cc
    campaign/dv-cl-campaign-collector.cc
  LIBRARIES_TO_LINK ${libdv-cl}
)
