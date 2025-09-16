# from a Python REPL or debugger
import migrator

# See or tweak the defaults
# print("Default debug arguments:")
# print(migrator.debug_argv())

# Run with custom arguments
migrator.run([
    "--src-config", r".\.influx.plungecaster-old.toml",
    "--dst-config", r".\.influx.metsys-node1.toml",
    "--src-bucket", "plungecaster",
    "--dst-bucket", "plungecaster",
    # Cast 1
    # "--start", "2025-03-17T08:00:00Z",
    # "--stop", "2025-03-21T08:00:00Z",
    # Cast 2
    # "--start", "2025-04-04T08:00:00Z",
    # "--stop", "2025-04-09T08:00:00Z",
    # Cast 3
    # "--start", "2025-04-11T08:00:00Z",
    # "--stop", "2025-04-15T18:00:00Z",
    # Cast 4
    # "--start", "2025-05-08T08:00:00Z",
    # "--stop", "2025-05-09T18:00:00Z",
    # Cast 5
    # "--start", "2025-05-14T12:00:00Z",
    # "--stop", "2025-05-14T19:00:00Z",
    # Cast 6
    # "--start", "2025-07-22T15:00:00Z",
    # "--stop", "2025-07-24T08:00:00Z",
    # All
    "--start", "2025-03-15T08:00:00Z",
    "--stop", "2025-09-12T20:00:00Z",
    # "--measurement", "temperature",
    "--measurement-regex", "^(heaters|sensors|pressure|temperature|relay_duty_cycle)$",
    "--tag-map", "id=heaters*->control",
    "--tag-map", "id=relay_duty_cycle*->control",
    "--tag-map", "device=PlungeCaster_Heater_ADSClient->CX-68ABF8",
    "--tag-inject", "env=production",
    "--tag-inject", "group=plungecaster",
    "--tag-inject", "source=plunge_caster_heater_control_CX68ABF8",
    "--tag-inject", "panel_name=Plunge Caster Heater Control",
    "--measurement-map", "heaters->control",
    "--measurement-map", "relay_duty_cycle->control",
    # "--verbose", # Enable verbose logging
    # "--dry-run",  # Enable dry-run to log points without writing
    # "--verify",  # Only query and print a count per window; do not write
])

# IO_Internal_AI_PV.TT_LHT_1 {datatype="float", device="PlungeCaster_Heater_ADSClient", id="temperature/IO_Internal_AI_PV.TT_LHT_1"}
# IO_Internal_AI_PV.DUTY_CYCLE_5 {datatype="float", device="CX-68ABF8", environment="live", group="plungecaster", id="control/IO_Internal_AI_PV.DUTY_CYCLE_5", name="control", panel_name="Plunge Caster Heater Control", source="plunge_caster_heater_control_CX68ABF8"}


# migrator.run([
#     "--src-config", r".\.influx.plungecaster-old.toml",
#     "--dst-config", r".\.influx.metsys-node1.toml",
#     "--src-bucket", "plungecaster",
#     "--dst-bucket", "plungecaster",
#     "--start", "2025-05-12T10:00:00Z",
#     "--stop", "2025-05-14T12:00:00Z",
#     "--measurement", "heaters",
#     "--tag-map", "id=heaters*->control",
#     "--tag-map", "device=PlungeCaster_Heater_ADSClient->CX-68ABF8",
#     "--tag-inject", "env=production",
#     "--tag-inject", "group=plungecaster",
#     "--tag-inject", "cast_number=2",
#     "--tag-inject", "source=plunge_caster_heater_control_CX68ABF8",
#     "--tag-inject", "panel_name=Plunge Caster Heater Control",
#     "--measurement-map", "heaters->control",
#     # "--verbose", # Enable verbose logging
#     # "--dry-run",  # Enable dry-run to log points without writing
#     # "--verify",  # Only query and print a count per window; do not write
# ])