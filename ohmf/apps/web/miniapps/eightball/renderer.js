import * as THREE from "./vendor/three.module.js";

const TABLE_TEXTURE_WIDTH = 1771;
const TABLE_TEXTURE_HEIGHT = 980;
const TABLE_ASPECT = TABLE_TEXTURE_WIDTH / TABLE_TEXTURE_HEIGHT;
const TABLE_WIDTH = 20;
const TABLE_HEIGHT = TABLE_WIDTH / TABLE_ASPECT;
const BALL_RADIUS = 0.34;
const BALL_SEGMENTS = 24;
const BALL_FLOAT = 0.06;
const ASSET_VERSION = (() => {
  try {
    const currentUrl = new URL(import.meta.url);
    return (
      currentUrl.searchParams.get("asset_version") ||
      window.OHMF_RUNTIME_CONFIG?.asset_version ||
      "dev"
    );
  } catch {
    return window.OHMF_RUNTIME_CONFIG?.asset_version || "dev";
  }
})();

function assetUrl(relativePath) {
  const url = new URL(relativePath, import.meta.url);
  url.searchParams.set("asset_version", ASSET_VERSION);
  return url.toString();
}

const COLLISION_PART_TEXTURE_URLS = Object.freeze({
  cornerLargeTopLeft: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-large-top-left.png"),
  cornerLargeMidLeft: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-large-mid-left.png"),
  cornerLargeBottomLeft: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-large-bottom-left.png"),
  centerMedium: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-medium-mid.png"),
  cornerSmallTopRight: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-small-top-right.png"),
  cornerSmallBottomRight: assetUrl("./assets/render/extracted/table-parts/table-base/pocket-arc-small-bottom-right.png"),
});
const BALL_TEXTURE_URLS = Object.freeze(
  Object.fromEntries(
    Array.from({ length: 16 }, (_, index) => [
      index === 0 ? "cue" : String(index),
      new URL(`./assets/source/materials/ball-maps/ball${index}-ipadhd.png`, import.meta.url).toString(),
    ])
  )
);

function sanitizeText(value, limit = 180) {
  return String(value || "").replace(/[\u0000-\u001f\u007f]/g, "").trim().slice(0, limit);
}

function toScenePosition(xPercent, yPercent) {
  return {
    x: (xPercent / 100 - 0.5) * TABLE_WIDTH,
    y: (0.5 - yPercent / 100) * TABLE_HEIGHT,
  };
}

function loadTexture(loader, url) {
  return new Promise((resolve, reject) => {
    loader.load(url, resolve, undefined, () => reject(new Error(`Failed to load texture: ${url}`)));
  });
}

export function createEightballRenderer({ canvas, statusEl }) {
  const renderer = new THREE.WebGLRenderer({
    canvas,
    alpha: true,
    antialias: false,
    preserveDrawingBuffer: true,
    powerPreference: "low-power",
  });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.25));
  renderer.outputColorSpace = THREE.SRGBColorSpace;

  const scene = new THREE.Scene();
  const camera = new THREE.OrthographicCamera(
    -TABLE_WIDTH / 2,
    TABLE_WIDTH / 2,
    TABLE_HEIGHT / 2,
    -TABLE_HEIGHT / 2,
    0.1,
    100
  );
  camera.position.set(0, 0, 12);
  camera.lookAt(0, 0, 0);

  scene.add(new THREE.AmbientLight(0xffffff, 1.35));
  const keyLight = new THREE.DirectionalLight(0xfff1cf, 1.15);
  keyLight.position.set(-6, 6, 12);
  scene.add(keyLight);

  const loader = new THREE.TextureLoader();
  const state = {
    tableBackingMesh: null,
    tableMesh: null,
    tablePocketMesh: null,
    cueMesh: null,
    aimLine: null,
    cueContactMarker: null,
    pocketMeshes: [],
    ballMeshes: new Map(),
    texturesReady: false,
    loadingPromise: null,
  };

  function setStatus(message) {
    if (!statusEl) return;
    const text = sanitizeText(message, 180);
    statusEl.hidden = !text;
    statusEl.textContent = text;
  }

  async function load() {
    if (state.texturesReady) return;
    if (state.loadingPromise) return state.loadingPromise;

    state.loadingPromise = (async () => {
      const maxAnisotropy = Math.max(1, renderer.capabilities.getMaxAnisotropy());
      const frameTexture = await loadTexture(loader, assetUrl("./assets/render/table/singletableLondon-frame.png"));
      frameTexture.colorSpace = THREE.SRGBColorSpace;
      frameTexture.anisotropy = Math.min(4, maxAnisotropy);

      const pocketTexture = await loadTexture(loader, assetUrl("./assets/render/table/singletableLondon-pockets.png"));
      pocketTexture.colorSpace = THREE.SRGBColorSpace;
      pocketTexture.anisotropy = Math.min(4, maxAnisotropy);

      const cueTexture = await loadTexture(loader, assetUrl("./assets/render/cue/Table_Standard_Cue-ipadhd.png"));
      cueTexture.colorSpace = THREE.SRGBColorSpace;
      cueTexture.anisotropy = Math.min(4, maxAnisotropy);

      const textureEntries = await Promise.all(
        Object.entries(BALL_TEXTURE_URLS).map(async ([key, url]) => {
          const texture = await loadTexture(loader, url);
          texture.colorSpace = THREE.SRGBColorSpace;
          texture.anisotropy = Math.min(4, maxAnisotropy);
          return [key, texture];
        })
      );

      const tableBacking = new THREE.Mesh(
        new THREE.PlaneGeometry(TABLE_WIDTH, TABLE_HEIGHT),
        new THREE.MeshBasicMaterial({ color: 0x090909 })
      );
      tableBacking.position.z = -0.25;
      scene.add(tableBacking);
      state.tableBackingMesh = tableBacking;

      const table = new THREE.Mesh(
        new THREE.PlaneGeometry(TABLE_WIDTH, TABLE_HEIGHT),
        new THREE.MeshBasicMaterial({ map: frameTexture, transparent: true })
      );
      table.position.z = -0.15;
      scene.add(table);
      state.tableMesh = table;

      const pocketOverlay = new THREE.Mesh(
        new THREE.PlaneGeometry(TABLE_WIDTH, TABLE_HEIGHT),
        new THREE.MeshBasicMaterial({ map: pocketTexture, transparent: true, depthWrite: false })
      );
      pocketOverlay.position.z = -0.2;
      scene.add(pocketOverlay);
      state.tablePocketMesh = pocketOverlay;

      const cueGeometry = new THREE.PlaneGeometry(7.2, 0.42);
      const cueMaterial = new THREE.MeshBasicMaterial({
        map: cueTexture,
        transparent: true,
        depthWrite: false,
      });
      const cue = new THREE.Mesh(cueGeometry, cueMaterial);
      cue.visible = false;
      cue.position.z = 0.12;
      scene.add(cue);
      state.cueMesh = cue;

      const aimGeometry = new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(0, 0, 0.02),
        new THREE.Vector3(0, 0, 0.02),
      ]);
      const aim = new THREE.Line(
        aimGeometry,
        new THREE.LineBasicMaterial({
          color: 0xfff2bf,
          transparent: true,
          opacity: 0.5,
        })
      );
      aim.visible = false;
      scene.add(aim);
      state.aimLine = aim;

      const contactMarker = new THREE.Mesh(
        new THREE.CircleGeometry(0.08, 20),
        new THREE.MeshBasicMaterial({
          color: 0xb91313,
          transparent: true,
          opacity: 0.95,
          depthWrite: false,
        })
      );
      contactMarker.visible = false;
      contactMarker.position.z = BALL_RADIUS + 0.02;
      scene.add(contactMarker);
      state.cueContactMarker = contactMarker;

      textureEntries.forEach(([key, texture]) => {
        const material = new THREE.MeshPhongMaterial({
          map: texture,
          shininess: 60,
          specular: new THREE.Color(0x4d4d4d),
        });
        const mesh = new THREE.Mesh(
          new THREE.SphereGeometry(BALL_RADIUS, BALL_SEGMENTS, BALL_SEGMENTS),
          material
        );
        mesh.visible = false;
        mesh.position.z = BALL_RADIUS - BALL_FLOAT;
        scene.add(mesh);
        state.ballMeshes.set(key, mesh);
      });

      state.texturesReady = true;
      state.loadingPromise = null;
      setStatus("");
    })().catch((error) => {
      state.loadingPromise = null;
      setStatus(error?.message || "3D assets failed to load.");
      throw error;
    });

    return state.loadingPromise;
  }

  function resize(width, height) {
    const safeWidth = Math.max(1, Math.round(width));
    const safeHeight = Math.max(1, Math.round(height));
    renderer.setSize(safeWidth, safeHeight, false);
    const viewportAspect = safeWidth / safeHeight;
    if (viewportAspect > TABLE_ASPECT) {
      const halfHeight = TABLE_HEIGHT / 2;
      const halfWidth = halfHeight * viewportAspect;
      camera.left = -halfWidth;
      camera.right = halfWidth;
      camera.top = halfHeight;
      camera.bottom = -halfHeight;
    } else {
      const halfWidth = TABLE_WIDTH / 2;
      const halfHeight = halfWidth / viewportAspect;
      camera.left = -halfWidth;
      camera.right = halfWidth;
      camera.top = halfHeight;
      camera.bottom = -halfHeight;
    }
    camera.updateProjectionMatrix();
  }

  function hideAllBalls() {
    state.ballMeshes.forEach((mesh) => {
      mesh.visible = false;
    });
  }

  function render(playfieldState) {
    const width = canvas.clientWidth || 1;
    const height = canvas.clientHeight || 1;
    resize(width, height);

    if (!state.texturesReady) {
      renderer.clear();
      setStatus("Loading 3D table assets...");
      return;
    }

    hideAllBalls();
    const balls = Array.isArray(playfieldState?.balls) ? playfieldState.balls : [];
    balls.forEach((entry) => {
      const key = entry.kind === "cue" ? "cue" : String(entry.key);
      const mesh = state.ballMeshes.get(key);
      if (!mesh) return;
      const position = toScenePosition(entry.x, entry.y);
      mesh.visible = true;
      mesh.position.set(position.x, position.y, BALL_RADIUS - BALL_FLOAT);
      mesh.rotation.set(-Math.PI, 0, Math.PI / 2);
      mesh.material.emissive = new THREE.Color(entry.isTarget ? 0x3d2f08 : 0x000000);
      mesh.material.emissiveIntensity = entry.isTarget ? 0.65 : 0;
    });

    const cueBall = balls.find((entry) => entry.kind === "cue");
    const cueMesh = state.cueMesh;
    const aimLine = state.aimLine;
    const cueContactMarker = state.cueContactMarker;
    if (cueBall && cueMesh && aimLine && playfieldState?.showCue) {
      const cuePoint = toScenePosition(cueBall.x, cueBall.y);
      const power = Math.max(0, Math.min(1, Number(playfieldState?.cuePower || 0) / 100));
      const angle = ((Number(playfieldState?.cueAngle || 0) || 0) * Math.PI) / 180;
      const reach = 9.8;
      const targetPoint = {
        x: cuePoint.x + Math.cos(angle) * reach,
        y: cuePoint.y + Math.sin(angle) * reach,
      };
      const inset = 0.8 + (1 - power) * 0.8;
      cueMesh.visible = true;
      cueMesh.position.set(
        cuePoint.x - Math.cos(angle) * inset,
        cuePoint.y - Math.sin(angle) * inset,
        0.12
      );
      cueMesh.rotation.z = angle;
      cueMesh.material.opacity = 0.88;

      const points = [
        new THREE.Vector3(cuePoint.x, cuePoint.y, 0.04),
        new THREE.Vector3(targetPoint.x, targetPoint.y, 0.04),
      ];
      aimLine.geometry.setFromPoints(points);
      aimLine.material.opacity = 0.22 + power * 0.45;
      aimLine.visible = true;

      if (cueContactMarker) {
        const spinX = Math.max(-1, Math.min(1, Number(playfieldState?.cueSpinX || 0)));
        const spinY = Math.max(-1, Math.min(1, Number(playfieldState?.cueSpinY || 0)));
        cueContactMarker.visible = true;
        cueContactMarker.position.set(
          cuePoint.x + spinX * BALL_RADIUS * 0.42,
          cuePoint.y + spinY * BALL_RADIUS * 0.42,
          BALL_RADIUS + 0.02
        );
      }
    } else {
      if (cueMesh) cueMesh.visible = false;
      if (aimLine) aimLine.visible = false;
      if (cueContactMarker) cueContactMarker.visible = false;
    }

    renderer.render(scene, camera);
    setStatus("");
  }

  function destroy() {
    if (state.aimLine) {
      state.aimLine.geometry.dispose();
      state.aimLine.material.dispose();
    }
    state.pocketMeshes.forEach((mesh) => {
      mesh.geometry.dispose();
      mesh.material.map?.dispose();
      mesh.material.dispose();
    });
    state.ballMeshes.forEach((mesh) => {
      mesh.geometry.dispose();
      mesh.material.dispose();
    });
    if (state.tableBackingMesh) {
      state.tableBackingMesh.geometry.dispose();
      state.tableBackingMesh.material.dispose();
    }
    if (state.tableMesh) {
      state.tableMesh.geometry.dispose();
      state.tableMesh.material.dispose();
    }
    if (state.tablePocketMesh) {
      state.tablePocketMesh.geometry.dispose();
      state.tablePocketMesh.material.dispose();
    }
    if (state.cueMesh) {
      state.cueMesh.geometry.dispose();
      state.cueMesh.material.dispose();
    }
    if (state.cueContactMarker) {
      state.cueContactMarker.geometry.dispose();
      state.cueContactMarker.material.dispose();
    }
    renderer.dispose();
  }

  return { load, render, resize, destroy };
}
