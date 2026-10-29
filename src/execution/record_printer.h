#pragma once

// Keep the template repository's path while reusing the target implementation.
// The target server streams Wire V3 query rows through QueryResultWriter in
// execution_manager.cpp; the legacy table printer remains buffer based.
#include "../record_printer.h"
