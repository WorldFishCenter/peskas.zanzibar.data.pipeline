import pandas as pd
from keplergl import KeplerGl 

def kepler_map(data_path):
    df = pd.read_csv(data_path)
    config = {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "df6ouho",
                        "type": "hexagon",
                        "config": {
                            "dataId": "wcs_surveys",
                            "label": "N. surveys",
                            "color": [255, 203, 153],
                            "highlightColor": [252, 242, 26, 255],
                            "columns": {
                                "lat": "lat",
                                "lng": "lon"
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.8,
                                "worldUnitSize": 0.5,
                                "resolution": 8,
                                "colorRange": {
                                    "name": "Uber Viz Diverging 1.5",
                                    "type": "diverging",
                                    "category": "Uber",
                                    "colors": ["#00939C", "#5DBABF", "#BAE1E2", "#F8C0AA", "#DD7755", "#C22E00"]
                                },
                                "coverage": 1,
                                "sizeRange": [0, 500],
                                "percentile": [0, 100],
                                "elevationPercentile": [0, 100],
                                "elevationScale": 20.1,
                                "enableElevationZoomFactor": True,
                                "colorAggregation": "count",
                                "sizeAggregation": "count",
                                "enable3d": True
                            },
                            "hidden": False,
                            "textLabel": [
                                {
                                    "field": None,
                                    "color": [255, 255, 255],
                                    "size": 18,
                                    "offset": [0, 0],
                                    "anchor": "start",
                                    "alignment": "center",
                                    "outlineWidth": 0,
                                    "outlineColor": [255, 0, 0, 255],
                                    "background": False,
                                    "backgroundColor": [0, 0, 200, 255]
                                }
                            ]
                        },
                        "visualChannels": {
                            "colorField": None,
                            "colorScale": "quantile",
                            "sizeField": None,
                            "sizeScale": "log"
                        }
                    }
                ],
                "effects": [],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "qjoiqmixj": [
                                {
                                    "name": "landing_site",
                                    "format": None
                                }
                            ]
                        },
                        "compareMode": False,
                        "compareType": "absolute",
                        "enabled": True
                    },
                    "brush": {
                        "size": 0.5,
                        "enabled": False
                    },
                    "geocoder": {
                        "enabled": False
                    },
                    "coordinate": {
                        "enabled": False
                    }
                },
                "layerBlending": "normal",
                "overlayBlending": "normal",
                "splitMaps": [],
                "animationConfig": {
                    "currentTime": None,
                    "speed": 1
                },
                "editor": {
                    "features": [],
                    "visible": True
                }
            },
            "mapState": {
                "bearing": 1.6774193548387109,
                "dragRotate": True,
                "latitude": -6.000201947432243,
                "longitude": 39.31378202497465,
                "pitch": 52.01158163186554,
                "zoom": 8.970123874737459,
                "isSplit": False,
                "isViewportSynced": True,
                "isZoomLocked": False,
                "splitMapViewports": []
            },
            "mapStyle": {
                "styleType": "om4051n",
                "topLayerGroups": {
                    "label": True,
                    "border": True,
                    "water": False
                },
                "visibleLayerGroups": {
                    "label": True,
                    "road": False,
                    "border": True,
                    "building": False,
                    "water": True
                },
                "threeDBuildingColor": [
                    194.6103322548211,
                    191.81688250953655,
                    185.2988331038727
                ],
                "mapStyles": {
                    "om4051n": {
                        "accessToken": None,
                        "custom": True,
                        "icon": "https://api.mapbox.com/styles/v1/langbart/cli8oua4m002a01pg17wt6vqa/static/-122.3391,37.7922,9,0,0/400x300?access_token=pk.eyJ1IjoidWNmLW1hcGJveCIsImEiOiJja2tyMjNhcWIwc29sMnVzMThoZ3djNXhzIn0._hfBNwCD7pCU7RAMOq6vUQ&logo=False&attribution=False",
                        "id": "om4051n",
                        "label": "Untitled",
                        "url": "mapbox://styles/langbart/cli8oua4m002a01pg17wt6vqa"
                    }
                }
            }
        }
    }
    
    kpmap = KeplerGl(height=400)
    kpmap.add_data(data=df, name='wcs_surveys')
    kpmap.config = config
    kpmap = KeplerGl(height=400, data={'wcs_surveys': df}, config=config)
    kpmap.save_to_html(data={'wcs_surveys': df}, config=config, file_name='kepler_wcs_map.html', read_only="True")
