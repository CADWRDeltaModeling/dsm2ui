"""DVue plugin registration for dsm2ui.

This module is imported by dvue at startup via the ``dvue.plugins`` entry point
group. It registers all dsm2ui readers with the ReaderRegistry.

To use with dvue, ensure dsm2ui is installed with the entry point:

    [project.entry-points."dvue.plugins"]
    dsm2ui = "dsm2ui.readers:register_readers"

Then readers are auto-discovered on startup:

    dvue ui run.h5 hist_qual.dss
"""


def register_readers():
    """Register all dsm2ui readers with dvue.
    
    Called automatically by dvue at startup via entry points.
    Registers readers for:
    - DSM2 HDF5 tidefiles (.h5, .hdf5)
    - DSM2 DSS output channels (.dss)
    - DSM2 echo input files (.inp)
    - Generic HEC-DSS browser (no extension, drag-drop only)
    """
    from dvue.registry import ReaderRegistry
    
    # Import reader classes from their modules
    # (deferred imports avoid circular dependency at startup if needed)
    from dsm2ui.dsm2ui import TidefileReader, DSM2DSSReader
    from dsm2ui.echo_plugin import DSM2EchoFileReader, DSM2BCFlowLoader
    from dsm2ui.dssui.dss_registry import DSSRegistryReader
    
    # Register DSM2 HDF5 tidefile reader
    ReaderRegistry.register(
        "dsm2_hdf5",
        TidefileReader,
        extensions=[".h5", ".hdf5"],
    )
    
    # Register DSM2 DSS output reader (DSM2-filtered C-parts only)
    ReaderRegistry.register(
        "dsm2_dss",
        DSM2DSSReader,
        extensions=[".dss"],
    )
    
    # Register DSM2 echo input file reader
    ReaderRegistry.register(
        "dsm2_echo_inp",
        DSM2EchoFileReader,
        extensions=[".inp"],
    )
    
    # Register DSM2 boundary condition flow loader (no extension)
    ReaderRegistry.register(
        "dsm2_bc_flow",
        DSM2BCFlowLoader,
    )
    
    # Register generic HEC-DSS browser (all C-parts, no extension filter)
    ReaderRegistry.register(
        "dss",
        DSSRegistryReader,
    )
