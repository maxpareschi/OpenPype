from Qt import QtWidgets, QtGui, QtCore


class LoadedInSceneDelegate(QtWidgets.QStyledItemDelegate):
    """Delegate for Loaded in Scene state columns.

    Shows "yes" or "no" for True or False values
    Colorizes green or dark grey based on True or False values

    """

    def __init__(self, *args, **kwargs):
        super(LoadedInSceneDelegate, self).__init__(*args, **kwargs)
        self._colors = {
            True: QtGui.QColor(80, 170, 80),
            False: QtGui.QColor(90, 90, 90)
        }

    def displayText(self, value, locale):
        return "yes" if value else "no"

    def initStyleOption(self, option, index):
        super(LoadedInSceneDelegate, self).initStyleOption(option, index)

        # Colorize based on value
        value = index.data(QtCore.Qt.DisplayRole)
        color = self._colors[bool(value)]
        option.palette.setBrush(QtGui.QPalette.Text, color)

class ApprovedVersionDelegate(QtWidgets.QStyledItemDelegate):
    """Delegate for Approved Version column.

    Colorizes green if last version is same as approved,
    Red if last version is bigger than approved

    """

    def __init__(self, version_column, *args, **kwargs):
        self.version_column = version_column
        super(ApprovedVersionDelegate, self).__init__(*args, **kwargs)
    
    def displayText(self, value, locale):
        if value:
            value = "v" + str(value).zfill(3)
        return value

    def initStyleOption(self, option, index):
        super(ApprovedVersionDelegate, self).initStyleOption(option, index)

        # Colorize based on value
        value = index.data(QtCore.Qt.DisplayRole)
        version = index.siblingAtColumn(
            self.version_column).data(QtCore.Qt.DisplayRole)
        self.main_version = 10
        color = QtGui.QColor(90, 90, 90)
        if value:
            if version < value:
                color = QtGui.QColor(220, 80, 80)
            elif version == value:
                color = QtGui.QColor(80, 220, 80)                
            else:
                color = QtGui.QColor(220, 190, 80)

        option.palette.setBrush(QtGui.QPalette.Text, color)
